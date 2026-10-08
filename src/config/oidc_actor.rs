use std::collections::BTreeSet;

use super::{AccessRegistry, Actor, Context, Result, bail};

impl AccessRegistry {
    pub fn actor_for_oidc(&self, subject: &str) -> Result<&Actor> {
        let mut matches = self.actors.iter().filter(|actor| {
            actor
                .oidc
                .as_ref()
                .and_then(|identity| identity.subject.as_deref())
                == Some(subject)
        });
        let actor = matches
            .next()
            .context("OIDC-пользователь не зарегистрирован в реестре доступа")?;
        if matches.next().is_some() {
            bail!("OIDC identity неоднозначно соответствует нескольким пользователям");
        }
        Ok(actor)
    }

    pub(super) fn validate_oidc_identities(&self) -> Result<()> {
        let mut subjects = BTreeSet::new();
        let mut usernames = BTreeSet::new();
        let mut emails = BTreeSet::new();
        for actor in &self.actors {
            let Some(identity) = &actor.oidc else {
                continue;
            };
            if identity.subject.is_none() && identity.username.is_none() && identity.email.is_none()
            {
                bail!(
                    "OIDC identity пользователя {} должен содержать subject, username или email",
                    actor.id
                );
            }
            for (field, value, values) in [
                ("subject", identity.subject.as_deref(), &mut subjects),
                ("username", identity.username.as_deref(), &mut usernames),
                ("email", identity.email.as_deref(), &mut emails),
            ] {
                let Some(value) = value else {
                    continue;
                };
                if value.trim().is_empty() {
                    bail!(
                        "OIDC {field} пользователя {} не может быть пустым",
                        actor.id
                    );
                }
                if !values.insert(value.to_owned()) {
                    bail!("OIDC {field}={value:?} должен быть уникальным");
                }
            }
        }
        Ok(())
    }

    /// Enforces the immutable identity boundary required by JWT/OIDC mode.
    ///
    /// `username` and `email` remain accepted as human-readable registry
    /// metadata, but neither is a stable security identifier: both may be
    /// renamed or reassigned by the identity provider. Only the issuer-scoped
    /// `sub` claim is used to grant an actor's roles and account access.
    pub(super) fn validate_jwt_oidc_bindings(&self) -> Result<()> {
        for actor in &self.actors {
            if actor
                .oidc
                .as_ref()
                .and_then(|identity| identity.subject.as_ref())
                .is_none()
            {
                bail!(
                    "OIDC identity пользователя {} должен содержать immutable subject при MCP_AUTH_MODE=jwt; username/email не используются для авторизации",
                    actor.id
                );
            }
        }
        Ok(())
    }
}
