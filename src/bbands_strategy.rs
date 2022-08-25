use barter::strategy::{Decision, Signal, SignalGenerator, SignalStrength};
use barter::data::MarketMeta;
use barter_data::model::{DataKind, MarketEvent};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ta::{indicators::BollingerBands, indicators::BollingerBandsOutput, Next};

/// Configuration for constructing a [`BBStrategy`] via the new() constructor method.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Config {
    pub bb_period: usize,
    pub bb_multiplier: f64,
    pub hedge_ratio: f64
}

#[derive(Clone, Debug)]
/// Example BB based strategy that implements [`SignalGenerator`].
pub struct BBStrategy {
    bb: BollingerBands,
    position: i32,
    previous_spread_close: f64,
    spread_close: f64,
    hedge_ratio: f64
}

impl SignalGenerator for BBStrategy {
    fn generate_signal(&mut self, market: &MarketEvent) -> Option<Signal> {
        // Check if it's a MarketEvent with a candle
        let spread_close = match &market.kind {
            DataKind::Candle(candle) => candle.volume, // highjacking volume param for now
            _ => return None,
        };
        let candle_close = match &market.kind {
            DataKind::Candle(candle) => candle.close,
            _ => return None,
        };

        // Calculate the next BB values using the new MarketEvent Candle data
        let bbo = self.bb.next(spread_close);
        
        // NOTE: i am using 0.0 as placeholder. This is not likely to cause an issue but possible if real candle close is 0.0 exact
        if self.spread_close == 0.0 {
            self.previous_spread_close = spread_close;
        } else {
            self.previous_spread_close = self.spread_close;
        }
        self.spread_close = spread_close;

        // Generate advisory signals map
        let signals = BBStrategy::generate_signals_map(self, bbo);

        // If signals map is empty, return no SignalEvent
        if signals.is_empty() {
            return None;
        }

        Some(Signal {
            time: Utc::now(),
            exchange: market.exchange.clone(),
            instrument: market.instrument.clone(),
            market_meta: MarketMeta {
                close: candle_close,
                time: market.exchange_time,
            },
            signals,
        })
    }
}

impl BBStrategy {
    /// Constructs a new [`BBStrategy`] component using the provided configuration struct.
    pub fn new(config: Config) -> Self {
        let bb_indicator = BollingerBands::new(config.bb_period, config.bb_multiplier)
            .expect("Failed to construct BB indicator");

        Self { bb: bb_indicator, previous_spread_close: 0.0, spread_close: 0.0, position: 0, hedge_ratio: config.hedge_ratio }
    }

    /// Given the latest BB values for a symbol, generates a map containing the [`SignalStrength`] for
    /// [`Decision`] under consideration.
    fn generate_signals_map(&mut self, bbo: BollingerBandsOutput) -> HashMap<Decision, SignalStrength> {
        let mut signals = HashMap::with_capacity(4);
        if self.position == 0 && self.previous_spread_close > bbo.upper && self.spread_close < bbo.upper {
            signals.insert(Decision::Short, BBStrategy::calculate_signal_strength(self));
            self.position = -1;
        } else
        if self.position == 0 && self.previous_spread_close < bbo.lower && self.spread_close > bbo.lower {
            signals.insert(Decision::Long, BBStrategy::calculate_signal_strength(self));
            self.position = 1;
        } else
        if self.position == -1 && self.spread_close < bbo.average {
            signals.insert(Decision::CloseShort, BBStrategy::calculate_signal_strength(self));
            self.position = 0;
        } else
        if self.position == 1 && self.spread_close > bbo.average {
            signals.insert(Decision::CloseLong, BBStrategy::calculate_signal_strength(self));
            self.position = 0;
        }
        signals
    }

    /// Calculates the [`SignalStrength`] of a particular [`Decision`].
    fn calculate_signal_strength(&mut self) -> SignalStrength {
        SignalStrength(self.hedge_ratio)
    }
}
