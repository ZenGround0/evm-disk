package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-data-segment/datasegment"
	inet "github.com/libp2p/go-libp2p/core/network"

	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:        "xchain",
		Description: "Filecoin Xchain Data Services",
		Usage:       "Export filecoin data storage to any blockchain",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Usage: "Path to the configuration file",
				Value: "~/.xchain/config.json",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "daemon",
				Usage: "Start the xchain adapter daemon",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						// TODO move to blockchain buffer service
						Name:  "buffer-service",
						Usage: "Run a buffer server",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "pdp-service",
						Usage: "Run a PDP storage server",
						Value: false,
					},
				},
				Action: func(cctx *cli.Context) error {
					isBuffer := cctx.Bool("buffer-service")
					isPDP := cctx.Bool("pdp-service")
					if !isBuffer && !isPDP { // default to running aggregator
						isPDP = true
					}

					cfg, err := LoadConfig(cctx.String("config"))
					if err != nil {
						log.Fatal(err)
					}

					g, ctx := errgroup.WithContext(cctx.Context)
					g.Go(func() error {
						if !isBuffer {
							return nil
						}
						path, err := homedir.Expand(cfg.BufferPath)
						if err != nil {
							return err
						}
						if err := os.MkdirAll(path, os.ModePerm); err != nil {
							return err
						}

						srv, err := NewBufferHTTPService(cfg.BufferPath)
						if err != nil {
							return &http.MaxBytesError{}
						}
						http.HandleFunc("/put", srv.PutHandler)
						http.HandleFunc("/get", srv.GetHandler)

						fmt.Printf("Server starting on port %d\n", cfg.BufferPort)
						server := &http.Server{
							Addr:    fmt.Sprintf("0.0.0.0:%d", cfg.BufferPort),
							Handler: nil, // http.DefaultServeMux
						}
						go func() {
							if err := server.ListenAndServe(); err != http.ErrServerClosed {
								log.Fatalf("Buffer HTTP server ListenAndServe: %v", err)
							}
						}()
						<-ctx.Done()

						// Context is cancelled, shut down the server
						return server.Shutdown(context.Background())
					})
					g.Go(func() error {
						if !isPDP {
							return nil
						}
						a, err := NewPDP(ctx, cfg)
						if err != nil {
							return err
						}
						return a.run(ctx)
					})
					return g.Wait()
				},
			},
		},
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		fmt.Println("Ctrl-c received. Shutting down...")
		os.Exit(0)
	}()

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type Config struct {
	ChainID       int
	Api           string
	OnRampAddress string
	ProverAddr    string
	KeyPath       string
	ClientAddr    string
	PayoutAddr    string
	OnRampABIPath string
	BufferPath    string
	BufferPort    int
	TransferIP    string
	TransferPort  int
	ProviderAddr  string
	LotusAPI      string
	TargetAggSize int
}

// Mirror OnRamp.sol's `Offer` struct
type Offer struct {
	CommP    []uint8        `json:"commP"`
	Size     uint64         `json:"size"`
	Location string         `json:"location"`
	Amount   *big.Int       `json:"amount"`
	Token    common.Address `json:"token"`
}

func (o *Offer) Piece() (filabi.PieceInfo, error) {
	pps := filabi.PaddedPieceSize(o.Size)
	if err := pps.Validate(); err != nil {
		return filabi.PieceInfo{}, err
	}
	_, c, err := cid.CidFromBytes(o.CommP)
	if err != nil {
		return filabi.PieceInfo{}, err
	}
	return filabi.PieceInfo{
		Size:     pps,
		PieceCID: c,
	}, nil
}

type pdpService struct {
	client     *ethclient.Client   // raw client for log subscriptions
	onramp     *bind.BoundContract // onramp binding over raw client for message sending
	auth       *bind.TransactOpts  // auth for message sending
	abi        *abi.ABI            // onramp abi for log subscription and message sending
	onrampAddr common.Address      // onramp address for log subscription
	proverAddr common.Address      // prover address for client contract deal
	payoutAddr common.Address      // aggregator payout address for receiving funds
	ch         chan DataReadyEvent // pass events to seperate goroutine for processing
	// TODO unclear if we will do pull or push transfers with PDP transfer
	transfers    map[int]AggregateTransfer // track aggregate data awaiting transfer
	transferLk   sync.RWMutex              // Mutex protecting transfers map
	transferID   int                       // ID of the next transfer
	transferAddr string                    // address to listen for transfer requests
	host         host.Host                 // libp2p host for deal protocol to boost
	// TODO we might just hardcode some of these
	// or alternatively do a non-lotus api requiring call to get state
	// for now comment out dealAddr
	//	spDealAddr  *peer.AddrInfo  // address to reach boost (or other) deal v 1.2 provider
	spActorAddr address.Address // address of the storage provider actor
}

func NewPDP(ctx context.Context, cfg *Config) (*pdpService, error) {
	client, err := ethclient.Dial(cfg.Api)
	if err != nil {
		log.Fatal(err)
	}

	parsedABI, err := LoadAbi(cfg.OnRampABIPath)
	if err != nil {
		return nil, err
	}
	proverContractAddress := common.HexToAddress(cfg.ProverAddr)
	onRampContractAddress := common.HexToAddress(cfg.OnRampAddress)
	payoutAddress := common.HexToAddress(cfg.PayoutAddr)
	onramp := bind.NewBoundContract(onRampContractAddress, *parsedABI, client, client, client)

	auth, err := loadPrivateKey(cfg)
	if err != nil {
		return nil, err
	}
	// TODO consider allowing config to specify listen addr and pid, for now it shouldn't matter as boost will entertain anybody
	h, err := libp2p.New()
	if err != nil {
		return nil, err
	}

	// Get maddr for dialing boost from on chain miner actor
	providerAddr, err := address.NewFromString(cfg.ProviderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse provider address: %w", err)
	}

	return &pdpService{
		client:       client,
		onramp:       onramp,
		onrampAddr:   onRampContractAddress,
		proverAddr:   proverContractAddress,
		payoutAddr:   payoutAddress,
		auth:         auth,
		ch:           make(chan DataReadyEvent, 1024), // buffer many events since consumer sometimes waits for chain
		transfers:    make(map[int]AggregateTransfer),
		transferLk:   sync.RWMutex{},
		transferAddr: fmt.Sprintf("%s:%d", cfg.TransferIP, cfg.TransferPort),
		abi:          parsedABI,
		host:         h,
		spActorAddr:  providerAddr,
	}, nil
}

// Run the two offerTaker persistant process
//  1. a goroutine listening for new DataReady events
//  2. a goroutine collecting data and aggregating before commiting
//     to store and sending to filecoin boost
func (a *pdpService) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Start listening for events
	// New DataReady events are passed through the channel to aggregation handling
	g.Go(func() error {
		query := ethereum.FilterQuery{
			Addresses: []common.Address{a.onrampAddr},
			Topics:    [][]common.Hash{{a.abi.Events["DataReady"].ID}},
		}

		err := a.SubscribeQuery(ctx, query)
		for err == nil || strings.Contains(err.Error(), "read tcp") {
			if err != nil {
				log.Printf("ignoring mystery error: %s", err)
			}
			if ctx.Err() != nil {
				err = ctx.Err()
				break
			}
			err = a.SubscribeQuery(ctx, query)
		}
		fmt.Printf("context done exiting subscribe query\n")
		return err
	})

	// Start aggregatation event handling
	g.Go(func() error {
		return a.runAggregate(ctx)
	})

	// Start handling data transfer requests
	g.Go(func() error {
		http.HandleFunc("/", a.transferHandler)
		fmt.Printf("Server starting on port %d\n", transferPort)
		server := &http.Server{
			Addr:    a.transferAddr,
			Handler: nil, // http.DefaultServeMux
		}
		go func() {
			if err := server.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatalf("Transfer HTTP server ListenAndServe: %v", err)
			}
		}()
		<-ctx.Done()
		fmt.Printf("context done about to shut down server\n")
		// Context is cancelled, shut down the server
		return server.Shutdown(context.Background())
	})

	return g.Wait()
}

const (
	// PODSI aggregation uses 64 extra bytes per piece
	// pieceOverhead = uint64(64) TODO uncomment this when we are smarter about determining threshold crossing
	// Piece CID of small valid car (below) that must be prepended to the aggregation for deal acceptance
	prefixCARCid = "baga6ea4seaqiklhpuei4wz7x3wwpvnul3sscfyrz2dpi722vgpwlolfky2dmwey"
	// Hex of the prefix car file
	prefixCAR = "3aa265726f6f747381d82a58250001701220b9ecb605f194801ee8a8355014e7e6e62966f94ccb6081" +
		"631e82217872209dae6776657273696f6e014101551220704a26a32a76cf3ab66ffe41eb27adefefe9c93206960bb0" +
		"147b9ed5e1e948b0576861744966487567684576657265747449494957617352696768743f5601701220b9ecb605f1" +
		"94801ee8a8355014e7e6e62966f94ccb6081631e82217872209dae122c0a2401551220704a26a32a76cf3ab66ffe41" +
		"eb27adefefe9c93206960bb0147b9ed5e1e948b012026576181d0a020801"
	// Size of the padded prefix car in bytes
	prefixCARSizePadded = uint64(256)
	// Data transfer port
	transferPort = 1728
	// libp2p identifier for latest deal protocol
	DealProtocolv120 = "/fil/storage/mk/1.2.0"
	// Delay to start deal at. For 2k devnet 4 second block time this is 13.3 minutes TODO Config
	dealDelayEpochs = 200
	// Storage deal duration, TODO figure out what to do about this, either comes from offer or config
	dealDuration = 518400 // 6 months (on mainnet)
)

func (a *pdpService) runPDP(ctx context.Context) error {
	// For now no batching, just send an add to PDP RPC for every onramp piece we see
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("ctx done shutting down xchain pdp storage")
			return nil
		case latestEvent := <-a.ch:
			// Check if the offer is too big to fit in a valid aggregate on its own
			// TODO: as referenced below there must be a better way when we introspect on the gory details of NewAggregate
			latestPiece, err := latestEvent.Offer.Piece()
			if err != nil {
				log.Printf("skipping offer %d, size %d not valid padded piece size ", latestEvent.OfferID, latestEvent.Offer.Size)
				continue
			}

			// TODO we'll want to change the name of commitAggregate in the onramp to commitPDP or something
			// The method signature should change too to take a single ID and no longer take inclusion proofs
			tx, err := a.onramp.Transact(a.auth, "commitAggregate", latestPiece.PieceCID.Bytes(), latestEvent.OfferID, a.payoutAddr)
			if err != nil {
				return err
			}
			receipt, err := bind.WaitMined(ctx, a.client, tx)
			if err != nil {
				return err
			}
			log.Printf("Tx %s committing aggregate commp %s included: %d", tx.Hash().Hex(), aggCommp.String(), receipt.Status)

			// Schedule aggregate data for transfer
			// After adding to the map this is now served in aggregator.transferHandler at `/?id={transferID}`
			locations := make([]string, len(pending))
			for i, event := range pending {
				locations[i] = event.Offer.Location
			}
			var transferID int
			a.transferLk.Lock()
			transferID = a.transferID
			a.transfers[transferID] = AggregateTransfer{
				locations: locations,
				agg:       agg,
			}
			a.transferID++
			a.transferLk.Unlock()
			log.Printf("Transfer ID %d scheduled for aggregate %s", transferID, aggCommp.String())

			err = a.sendDeal(ctx, aggCommp, transferID)
			if err != nil {
				log.Printf("[ERROR] failed to send deal: %s", err)
			}

			// Reset queue to empty, add the event that triggered aggregation
			pending = pending[:0]
			pending = append(pending, latestEvent)
		}
	}
}

// Send deal data to the configured SP deal making address (boost node)
// The deal is made with the configured prover client contract
// Heavily inspired by boost client
func (a *pdpService) sendDeal(ctx context.Context, CommP cid.Cid, transferID int) error {
	// TODO PDP Service RPC add endpoint call
	return nil
}

// This might end up being useful still
func doRpc(ctx context.Context, s inet.Stream, req interface{}, resp interface{}) error {
	errc := make(chan error)
	go func() {
		if err := cborutil.WriteCborRPC(s, req); err != nil {
			errc <- fmt.Errorf("failed to send request: %w", err)
			return
		}

		if err := cborutil.ReadCborRPC(s, resp); err != nil {
			errc <- fmt.Errorf("failed to read response: %w", err)
			return
		}

		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LazyHTTPReader is an io.Reader that fetches data from an HTTP URL on the first Read call
type lazyHTTPReader struct {
	url     string
	reader  io.ReadCloser
	started bool
}

func (l *lazyHTTPReader) Read(p []byte) (int, error) {
	if !l.started {
		// Start the HTTP request on the first Read call
		fmt.Printf("reading %s\n", l.url)
		resp, err := http.Get(l.url)
		if err != nil {
			return 0, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return 0, fmt.Errorf("failed to fetch data: %s", resp.Status)
		}
		l.reader = resp.Body
		l.started = true
	}
	return l.reader.Read(p)
}

func (l *lazyHTTPReader) Close() error {
	if l.reader != nil {
		return l.reader.Close()
	}
	return nil
}

// Handle data transfer requests from boost
// TODO unclear if we're dealing wit hpull or push transfer these days
func (a *pdpService) transferHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	// TODO if we are doing pull + content length we'll need a different way to do this
	//w.Header().Set("Content-Length", strconv.Itoa(int(a.targetDealSize-a.targetDealSize/128)))
	if r.Method == "HEAD" {
		w.WriteHeader(http.StatusOK)
		return
	}

	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "ID is required", http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	a.transferLk.RLock()
	transfer, ok := a.transfers[id]
	a.transferLk.RUnlock()
	if !ok {
		http.Error(w, "No data found", http.StatusNotFound)
		return
	}
	// First write the CAR prefix to the response
	prefixCARBytes, err := hex.DecodeString(prefixCAR)
	if err != nil {
		http.Error(w, "Failed to decode CAR prefix", http.StatusInternalServerError)
		return
	}

	readers := []io.Reader{bytes.NewReader(prefixCARBytes)}
	// Fetch each sub piece from its buffer location and write to response
	for _, url := range transfer.locations {
		lazyReader := &lazyHTTPReader{url: url}
		readers = append(readers, lazyReader)
		defer lazyReader.Close()
	}
	// ideas
	// looks like aggReader is returning more bytes than the provided content-length.
	// either aggregate reader has an error or content-length has an error
	// since hashing doesn't lie its probably content length.  Lets figure out what the actual length is
	// and why this only bytes us past 8 MiB
	aggReader, err := transfer.agg.AggregateObjectReader(readers)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create aggregate reader: %s", err), http.StatusInternalServerError)
		return
	}
	_, err = io.Copy(w, aggReader)
	if err != nil {
		log.Printf("failed to write aggregate stream: %s", err)
	}
}

type AggregateTransfer struct {
	locations []string
	agg       *datasegment.Aggregate
}

func (a *pdpService) SubscribeQuery(ctx context.Context, query ethereum.FilterQuery) error {
	logs := make(chan types.Log)
	log.Printf("Listening for data ready events on %s\n", a.onrampAddr.Hex())
	sub, err := a.client.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case err := <-sub.Err():
			return err
		case vLog := <-logs:
			event, err := parseDataReadyEvent(vLog, a.abi)
			if err != nil {
				return err
			}
			log.Printf("Sending offer %d for aggregation\n", event.OfferID)
			// This is where we should make packing decisions.
			// In the current prototype we accept all offers regardless
			// of payment type, amount or duration
			a.ch <- *event
		}
	}
	return nil
}

// Define a Go struct to match the DataReady event from the OnRamp contract
type DataReadyEvent struct {
	Offer   Offer
	OfferID uint64
}

// Function to parse the DataReady event from log data
func parseDataReadyEvent(log types.Log, abi *abi.ABI) (*DataReadyEvent, error) {
	eventData, err := abi.Unpack("DataReady", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack 'DataReady' event: %w", err)
	}

	// Assuming eventData is correctly ordered as per the event definition in the Solidity contract
	if len(eventData) != 2 {
		return nil, fmt.Errorf("unexpected number of fields for 'DataReady' event: got %d, want 2", len(eventData))
	}

	offerID, ok := eventData[1].(uint64)
	if !ok {
		return nil, fmt.Errorf("invalid type for offerID, expected uint64, got %T", eventData[1])
	}

	offerDataRaw := eventData[0]
	// JSON round trip to deserialize to offer
	bs, err := json.Marshal(offerDataRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal raw offer data to json: %w", err)
	}
	var offer Offer
	err = json.Unmarshal(bs, &offer)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal raw offer data to nice offer struct: %w", err)
	}

	return &DataReadyEvent{
		OfferID: offerID,
		Offer:   offer,
	}, nil
}

func HandleOffer(offer *Offer) error {
	return nil
}

// Load Config given path to JSON config file
func LoadConfig(path string) (*Config, error) {
	path, err := homedir.Expand(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	bs, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config bytes from file: %w", err)
	}
	var cfg []Config

	err = json.Unmarshal(bs, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode config file: %v", err)
	}
	if len(cfg) != 1 {
		return nil, fmt.Errorf("expected 1 config, got %d", len(cfg))
	}
	return &cfg[0], nil
}

// Load and unlock the keystore with XCHAIN_PASSPHRASE env var
// return a transaction authorizer
func loadPrivateKey(cfg *Config) (*bind.TransactOpts, error) {
	path, err := homedir.Expand(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open keystore file: %w", err)
	}
	defer file.Close()
	keyJSON, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read key store bytes from file: %w", err)
	}

	// Create a temporary directory to initialize the per-call keystore
	tempDir, err := os.MkdirTemp("", "xchain-tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer os.RemoveAll(tempDir)
	ks := keystore.NewKeyStore(tempDir, keystore.StandardScryptN, keystore.StandardScryptP)

	// Import existing key
	a, err := ks.Import(keyJSON, os.Getenv("XCHAIN_PASSPHRASE"), os.Getenv("XCHAIN_PASSPHRASE"))
	if err != nil {
		return nil, fmt.Errorf("failed to import key %s: %w", cfg.ClientAddr, err)
	}
	if err := ks.Unlock(a, os.Getenv("XCHAIN_PASSPHRASE")); err != nil {
		return nil, fmt.Errorf("failed to unlock keystore: %w", err)
	}
	return bind.NewKeyStoreTransactorWithChainID(ks, a, big.NewInt(int64(cfg.ChainID)))
}

// Load contract abi at the given path
func LoadAbi(path string) (*abi.ABI, error) {
	path, err := homedir.Expand(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open abi file: %w", err)
	}
	parsedABI, err := abi.JSON(f)
	if err != nil {
		return nil, fmt.Errorf("failed to parse abi: %w", err)
	}
	return &parsedABI, nil
}
