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
}
