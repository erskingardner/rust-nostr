// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2024 Rust Nostr Developers
// Distributed under the MIT software license

use std::ops::Deref;
use std::sync::Arc;

use uniffi::Object;

use super::zapper::NostrZapper;
use super::{Client, Options};
use crate::database::NostrDatabase;
use crate::protocol::helper::unwrap_or_clone_arc;
use crate::protocol::signer::NostrSigner;

#[derive(Clone, Default, Object)]
pub struct ClientBuilder {
    inner: nostr_sdk::ClientBuilder,
}

impl From<nostr_sdk::ClientBuilder> for ClientBuilder {
    fn from(inner: nostr_sdk::ClientBuilder) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl ClientBuilder {
    /// New client builder
    #[uniffi::constructor]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn signer(self: Arc<Self>, signer: &NostrSigner) -> Self {
        let mut builder = unwrap_or_clone_arc(self);
        builder.inner = builder.inner.signer(signer.deref().clone());
        builder
    }

    pub fn zapper(self: Arc<Self>, zapper: &NostrZapper) -> Self {
        let mut builder = unwrap_or_clone_arc(self);
        builder.inner = builder.inner.zapper(zapper.deref().clone());
        builder
    }

    pub fn database(self: Arc<Self>, database: &NostrDatabase) -> Self {
        let mut builder = unwrap_or_clone_arc(self);
        builder.inner = builder.inner.database(database.deref().clone());
        builder
    }

    /// Set opts
    pub fn opts(self: Arc<Self>, opts: Arc<Options>) -> Self {
        let mut builder = unwrap_or_clone_arc(self);
        builder.inner = builder.inner.opts(opts.as_ref().deref().clone());
        builder
    }

    /// Build [`Client`]
    pub fn build(&self) -> Arc<Client> {
        let inner = self.inner.clone();
        Arc::new(inner.build().into())
    }
}
