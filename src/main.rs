use barter::{
    data::historical,
    engine::{trader::Trader, Engine},
    event::{Event, EventTx},
    execution::{
        simulated::{Config as ExecutionConfig, SimulatedExecution},
        Fees,
    },
    portfolio::{
        allocator::DefaultAllocator, portfolio::MetaPortfolio,
        repository::in_memory::InMemoryRepository, risk::DefaultRisk,
    },
    statistic::summary::{
        trading::{Config as StatisticConfig, TradingSummary},
        Initialiser,
    },
    strategy::example::{Config as StrategyConfig, RSIStrategy},
};
use barter_data::model::{Candle, DataKind, MarketEvent};
use barter_integration::model::{Exchange, Instrument, InstrumentKind, Market};
use chrono::{DateTime, NaiveDateTime, Utc};
use parking_lot::Mutex;
use std::{collections::HashMap, fs, sync::Arc, path::Path};
use tokio::sync::mpsc;
use uuid::Uuid;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use parquet::record::RowAccessor;
use parquet::schema::types::TypePtr;

#[tokio::main]
async fn main() {
    // Create channel to distribute Commands to the Engine & it's Traders (eg/ Command::Terminate)
    let (_command_tx, command_rx) = mpsc::channel(20);

    // Create Event channel to listen to all Engine Events in real-time
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_tx = EventTx::new(event_tx);

    // Generate unique identifier to associate an Engine's components
    let engine_id = Uuid::new_v4();

    // Resolution
    let resolution = "15m";

    // Create the Market(s) to be traded on (1-to-1 relationship with a Trader)
    let asset_a = "mana";
    let asset_b = "sand";
    let market_a = Market::new("binance", (asset_a, "usdt", InstrumentKind::FuturePerpetual));
    let market_b = Market::new("binance", (asset_b, "usdt", InstrumentKind::FuturePerpetual));

    // Build global shared-state MetaPortfolio (1-to-1 relationship with an Engine)
    let portfolio = Arc::new(Mutex::new(
        MetaPortfolio::builder()
            .engine_id(engine_id)
            .markets(vec![market_a.clone(), market_b.clone()])
            .starting_cash(10_000.0)
            .repository(InMemoryRepository::new())
            .allocation_manager(DefaultAllocator {
                default_order_value: 100.0,
            })
            .risk_manager(DefaultRisk {})
            .statistic_config(StatisticConfig {
                starting_equity: 10_000.0,
                trading_days_per_year: 365,
                risk_free_return: 0.0,
            })
            .build_and_init()
            .expect("failed to build & initialise MetaPortfolio"),
    ));

    // Build Trader(s)
    let mut traders = Vec::new();

    // Create channel for each Trader so the Engine can distribute Commands to it
    let (trader_a_command_tx, trader_a_command_rx) = mpsc::channel(10);
    let (trader_b_command_tx, trader_b_command_rx) = mpsc::channel(10);

    traders.push(
        Trader::builder()
            .engine_id(engine_id)
            .market(market_a.clone())
            .command_rx(trader_a_command_rx)
            .event_tx(event_tx.clone())
            .portfolio(Arc::clone(&portfolio))
            .data(historical::MarketFeed::new(
                load_parquet_market_event_candles(asset_a, resolution).into_iter(),
            ))
            .strategy(RSIStrategy::new(StrategyConfig { rsi_period: 14 }))
            .execution(SimulatedExecution::new(ExecutionConfig {
                simulated_fees_pct: Fees {
                    exchange: 0.1,
                    slippage: 0.05,
                    network: 0.0,
                },
            }))
            .build()
            .expect("failed to build trader"),
    );

    traders.push(
        Trader::builder()
            .engine_id(engine_id)
            .market(market_b.clone())
            .command_rx(trader_b_command_rx)
            .event_tx(event_tx.clone())
            .portfolio(Arc::clone(&portfolio))
            .data(historical::MarketFeed::new(
                load_parquet_market_event_candles(asset_b, resolution).into_iter(),
            ))
            .strategy(RSIStrategy::new(StrategyConfig { rsi_period: 14 }))
            .execution(SimulatedExecution::new(ExecutionConfig {
                simulated_fees_pct: Fees {
                    exchange: 0.1,
                    slippage: 0.05,
                    network: 0.0,
                },
            }))
            .build()
            .expect("failed to build trader"),
    );

    // Build Engine (1-to-many relationship with Traders)
    // Create HashMap<Market, trader_command_tx> so Engine can route Commands to Traders
    let trader_command_txs = HashMap::from([(market_a, trader_a_command_tx), (market_b, trader_b_command_tx)]);

    let engine = Engine::builder()
        .engine_id(engine_id)
        .command_rx(command_rx)
        .portfolio(portfolio)
        .traders(traders)
        .trader_command_txs(trader_command_txs)
        .statistics_summary(TradingSummary::init(StatisticConfig {
            starting_equity: 1000.0,
            trading_days_per_year: 365,
            risk_free_return: 0.0,
        }))
        .build()
        .expect("failed to build engine");

    // Run Engine trading & listen to Events it produces
    tokio::spawn(listen_to_engine_events(event_rx));
    engine.run().await;
}

fn read_parquet(in_path: &Path) -> (Vec<Row>, TypePtr) {
    // Read Parquet input file. Return a vector of rows and the Schema
    let file = fs::File::open(in_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let row_iter = reader.get_row_iter(None).unwrap();
    let num_rows = reader.metadata().file_metadata().num_rows();
    let rows: Vec<Row> = row_iter.collect();
    println!("num rows: {}", num_rows);

    let schema = reader.metadata().file_metadata().schema_descr().root_schema_ptr();
    (rows, schema)
}

fn load_parquet_market_event_candles(asset: &str, resolution: &str) -> Vec<MarketEvent> {
    let parquet_path = format!("./data/{}USDT_{}.parquet.gzip", asset.to_uppercase(), resolution);
    
    let (parquet_rows, _) = read_parquet(&Path::new(&parquet_path));

    let candles: Vec<Candle> = parquet_rows
                                .into_iter()
                                .map(|row| Candle {
                                    start_time: DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(row.get_long(0).unwrap() / 1000, 0), Utc),
                                    end_time: DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(row.get_long(6).unwrap() / 1000, 0), Utc),
                                    open: row.get_string(1).unwrap().parse::<f64>().unwrap(),
                                    high: row.get_string(2).unwrap().parse::<f64>().unwrap(),
                                    low: row.get_string(3).unwrap().parse::<f64>().unwrap(),
                                    close: row.get_string(4).unwrap().parse::<f64>().unwrap(),
                                    volume: row.get_string(5).unwrap().parse::<f64>().unwrap(),
                                    trade_count: row.get_long(8).unwrap() as u64
                                })
                                .collect();

    candles
        .into_iter()
        .map(|candle| MarketEvent {
            exchange_time: candle.end_time,
            received_time: Utc::now(),
            exchange: Exchange::from("binance"),
            instrument: Instrument::from((asset, "usdt", InstrumentKind::FuturePerpetual)),
            kind: DataKind::Candle(candle),
        })
        .collect()
}

// Listen to Events that occur in the Engine. These can be used for updating event-sourcing,
// updating dashboard, etc etc.
async fn listen_to_engine_events(mut event_rx: mpsc::UnboundedReceiver<Event>) {
    while let Some(event) = event_rx.recv().await {
        match event {
            Event::Market(_) => {
                // Market Event occurred in Engine
            }
            Event::Signal(signal) => {
                // Signal Event occurred in Engine
                println!("{signal:?}");
            }
            Event::SignalForceExit(_) => {
                // SignalForceExit Event occurred in Engine
            }
            Event::OrderNew(new_order) => {
                // OrderNew Event occurred in Engine
                println!("{new_order:?}");
            }
            Event::OrderUpdate => {
                // OrderUpdate Event occurred in Engine
            }
            Event::Fill(fill_event) => {
                // Fill Event occurred in Engine
                println!("{fill_event:?}");
            }
            Event::PositionNew(new_position) => {
                // PositionNew Event occurred in Engine
                println!("{new_position:?}");
            }
            Event::PositionUpdate(updated_position) => {
                // PositionUpdate Event occurred in Engine
                println!("{updated_position:?}");
            }
            Event::PositionExit(exited_position) => {
                // PositionExit Event occurred in Engine
                println!("{exited_position:?}");
            }
            Event::Balance(balance_update) => {
                // Balance update Event occurred in Engine
                println!("{balance_update:?}");
            }
        }
    }
}
