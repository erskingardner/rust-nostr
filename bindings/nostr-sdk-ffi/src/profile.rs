// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2024 Rust Nostr Developers
// Distributed under the MIT software license

use std::ops::Deref;
use std::sync::Arc;

use nostr_sdk::prelude;
use uniffi::Object;

use crate::protocol::{Metadata, PublicKey};

#[derive(Debug, PartialEq, Eq, Hash, Object)]
#[uniffi::export(Debug, Eq, Hash)]
pub struct Profile {
    inner: prelude::Profile,
}

impl From<prelude::Profile> for Profile {
    fn from(inner: prelude::Profile) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl Profile {
    /// Compose new profile
    #[uniffi::constructor]
    pub fn new(public_key: &PublicKey, metadata: Arc<Metadata>) -> Self {
        Self {
            inner: prelude::Profile::new(**public_key, metadata.as_ref().deref().clone()),
        }
    }

    /// Get profile public key
    pub fn public_key(&self) -> Arc<PublicKey> {
        Arc::new(self.inner.public_key().into())
    }

    /// Get profile metadata
    pub fn metadata(&self) -> Arc<Metadata> {
        Arc::new(self.inner.metadata().into())
    }

    /// Get profile name
    ///
    /// Steps (go to next step if field is `None` or `empty`):
    /// * Check `display_name` field
    /// * Check `name` field
    /// * Return cutted public key (ex. `00000000:00000002`)
    pub fn name(&self) -> String {
        self.inner.name()
    }
}
