use super::{SearchEngine, SearchState};
use async_trait::async_trait;
use atuin_client::{database::Database, history::History, settings::FilterMode};
use eyre::Result;
use itertools::Itertools;
use nucleo::{
    Config, Nucleo,
    pattern::{CaseMatching, Normalization},
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;
use tokio::sync::Notify;

pub struct Search {
    all_history: Arc<Mutex<Vec<History>>>,
    nucleo: Nucleo<History>,
    last_filter_mode: Option<FilterMode>,
    refresh_notify: Arc<Notify>,
    refresh_pending: Arc<AtomicBool>,
}

impl Search {
    pub fn new() -> Self {
        let nucleo = Nucleo::new(Config::DEFAULT.match_paths(), Arc::new(|| {}), None, 1);

        Self {
            all_history: Arc::new(Mutex::new(Vec::new())),
            refresh_notify: Arc::new(Notify::new()),
            refresh_pending: Arc::new(AtomicBool::new(false)),
            last_filter_mode: None,
            nucleo,
        }
    }

    async fn refresh_results(
        &mut self,
        state: &SearchState,
        db: &mut dyn Database,
        input_empty: bool,
    ) {
        let needs_update = self.all_history.lock().unwrap().is_empty()
            || self.last_filter_mode != Some(state.filter_mode);
        if !needs_update {
            return;
        }
        self.last_filter_mode = Some(state.filter_mode);

        if !input_empty {
            let results = db
                .list(&[state.filter_mode], &state.context, None, true, false)
                .await
                .unwrap();
            *self.all_history.lock().unwrap() = results;
            return;
        }
        if self.refresh_pending.swap(false, Ordering::SeqCst) {
            self.refresh_notify.notified().await;
        }

        let db = dyn_clone::clone_box(db);
        self.refresh_pending.store(true, Ordering::SeqCst);
        self.last_filter_mode = Some(state.filter_mode);
        let filter_mode = state.filter_mode.clone();
        let context = state.context.clone();
        let refresh_notify = self.refresh_notify.clone();
        let all_history = self.all_history.clone();

        tokio::task::spawn(async move {
            let results = db
                .list(&[filter_mode], &context, None, true, false)
                .await
                .unwrap();
            *all_history.lock().unwrap() = results;
            refresh_notify.notify_one();
        });
    }
}

#[async_trait]
impl SearchEngine for Search {
    async fn query(&mut self, state: &SearchState, db: &mut dyn Database) -> Result<Vec<History>> {
        let input_empty = state.input.as_str().is_empty();
        self.refresh_results(state, db, input_empty).await;
        if input_empty {
            Ok(db
                .list(&[state.filter_mode], &state.context, Some(200), true, false)
                .await?)
        } else {
            self.full_query(state, db).await
        }
    }

    async fn full_query(
        &mut self,
        state: &SearchState,
        _db: &mut dyn Database,
    ) -> Result<Vec<History>> {
        self.nucleo.restart(false);
        let injector = self.nucleo.injector();

        for item in self.all_history.lock().unwrap().iter() {
            injector.push(item.clone(), |item, row| {
                row[0] = item.command.clone().into();
            });
        }
        self.nucleo.pattern.reparse(
            0,
            state.input.as_str(),
            CaseMatching::Smart,
            Normalization::Smart,
            false,
        );
        loop {
            let status = self.nucleo.tick(0);
            if !status.running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let snapshot = self.nucleo.snapshot();
        let matches = snapshot.matches();

        let now = OffsetDateTime::now_utc().unix_timestamp();
        let items = snapshot
            .matched_items(..)
            .take(200)
            .enumerate()
            .sorted_by(|(i1, item1), (i2, item2)| {
                let match_res1 = matches[*i1];
                let match_res2 = matches[*i2];

                let diff1 = now - item1.data.timestamp.unix_timestamp();
                let diff2 = now - item2.data.timestamp.unix_timestamp();
                (match_res1.score as f64 - (diff1 as f64).log2())
                    .partial_cmp(&(match_res2.score as f64 - (diff2 as f64).log2()))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .rev()
            .map(|(_, item)| item.data.clone());
        Ok(items.collect())
    }
}
