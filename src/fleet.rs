//! Temporary bots for other channels. The main bot stays in charge of routing: when its own
//! channel has people and someone elsewhere asks for music, it starts a temporary bot there.
//! All bots share one process, cache and Netease login; each has its own connection and queue.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::warn;
use tsclientlib::{ChannelId, ClientId};

use crate::bot::Bot;
use crate::config::Config;
use crate::player::Cache;
use crate::resolve::Resolver;
use crate::ts::{self, BotEvent, ConnCmd, Reply, Role};

struct Slot {
    channel: ChannelId,
    tx: mpsc::Sender<BotEvent>,
    /// Commands held while the bot connects; `None` once it is in its channel.
    early: Option<Vec<BotEvent>>,
}

struct Inner {
    cfg: Arc<Config>,
    resolver: Arc<Resolver>,
    cache: Arc<Cache>,
    http: reqwest::Client,
    stop: watch::Receiver<bool>,
    main_conn: mpsc::Sender<ConnCmd>,
    slots: Mutex<HashMap<usize, Slot>>,
}

#[derive(Clone)]
pub struct Fleet(Arc<Inner>);

impl Fleet {
    pub fn new(
        cfg: Arc<Config>,
        resolver: Arc<Resolver>,
        cache: Arc<Cache>,
        http: reqwest::Client,
        stop: watch::Receiver<bool>,
        main_conn: mpsc::Sender<ConnCmd>,
    ) -> Self {
        Fleet(Arc::new(Inner { cfg, resolver, cache, http, stop, main_conn, slots: Mutex::new(HashMap::new()) }))
    }

    pub fn temp_in(&self, channel: ChannelId) -> bool {
        self.0.slots.lock().unwrap().values().any(|s| s.channel == channel)
    }

    pub fn has_room(&self) -> bool {
        self.free_index().is_some()
    }

    fn free_index(&self) -> Option<usize> {
        let slots = self.0.slots.lock().unwrap();
        (2..=self.0.cfg.bot.max_bots).find(|i| !slots.contains_key(i))
    }

    /// Starts a temporary bot in `channel`. `first` runs once it has arrived.
    pub fn spawn(&self, channel: ChannelId, password: Option<String>, requester: ClientId, first: Option<BotEvent>) -> bool {
        let Some(index) = self.free_index() else { return false };
        let i = &self.0;
        let (audio_tx, audio_rx) = mpsc::channel(4);
        let (conn_tx, conn_rx) = mpsc::channel(64);
        let (bot_tx, bot_rx) = mpsc::channel(64);
        let busy = Arc::new(AtomicBool::new(false));
        let slot = Slot { channel, tx: bot_tx.clone(), early: Some(first.into_iter().collect()) };
        i.slots.lock().unwrap().insert(index, slot);

        let bot = Bot::new(i.cfg.clone(), i.resolver.clone(), i.cache.clone(), i.http.clone(), conn_tx, audio_tx, false, busy.clone());
        tokio::spawn(bot.run(bot_rx));
        let role = Role { index, fleet: self.clone(), channel, channel_password: password, requester: Some(requester), busy };
        let (cfg, stop, fleet) = (i.cfg.clone(), i.stop.clone(), self.clone());
        tokio::spawn(async move {
            if let Err(e) = ts::run(cfg, role, audio_rx, conn_rx, bot_tx, stop).await {
                warn!("temporary bot {index}: {e:#}");
            }
            fleet.remove(index);
        });
        true
    }

    /// Hands a command to the temporary bot in `channel`.
    pub async fn forward(&self, channel: ChannelId, ev: BotEvent) {
        let tx = {
            let mut slots = self.0.slots.lock().unwrap();
            let Some(slot) = slots.values_mut().find(|s| s.channel == channel) else { return };
            if let Some(early) = &mut slot.early {
                early.push(ev);
                return;
            }
            slot.tx.clone()
        };
        let _ = tx.send(ev).await;
    }

    pub fn set_channel(&self, index: usize, channel: ChannelId) {
        if let Some(slot) = self.0.slots.lock().unwrap().get_mut(&index) {
            slot.channel = channel;
        }
    }

    pub fn take_early(&self, index: usize) -> Vec<BotEvent> {
        self.0.slots.lock().unwrap().get_mut(&index).and_then(|s| s.early.take()).unwrap_or_default()
    }

    fn remove(&self, index: usize) {
        self.0.slots.lock().unwrap().remove(&index);
    }

    /// Tells someone something through the main bot, for bots that couldn't get connected.
    pub async fn notify(&self, who: ClientId, text: String) {
        let _ = self.0.main_conn.send(ConnCmd::Text(Reply::Client(who), text)).await;
    }

    /// Gives temporary bots a moment to say goodbye at shutdown.
    pub async fn wait_empty(&self, max: Duration) {
        let _ = tokio::time::timeout(max, async {
            while !self.0.slots.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
    }
}
