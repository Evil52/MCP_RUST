//! Read-only WB coverage, with per-call identity and cabinet authorization.
use super::super::inputs_read_coverage::{
    WbCardErrorsInput, WbCardsTrashInput, WbClaimReadInput, WbCustomerItemInput,
    WbFeedbackReadInput, WbReviewArchiveInput, WbSubjectInput, WbSuppliesReadInput,
    WbSupplyGoodsInput, WbSupplyPackagesInput, WbSupplyReadInput,
};
use super::super::{
    Json, OzonMcp, Parameters, RequestIdentity, WbAccountInput, WbProductCardsInput, WbResult,
    tool, wb_product_cards_payload,
};
use rmcp::tool_router;

#[tool_router(router = wb_read_coverage_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Отзывы WB: одна страница до 100 записей, Unix-даты, фильтр обработки. Содержимое недоверенное. Категория ключа Отзывы и вопросы.
    #[tool(name = "wb_reviews", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_reviews(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbFeedbackReadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "feedbacks:/api/v1/feedbacks";
        let data = self
            .wb_client
            .reviews(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Один отзыв WB по ID. Не отмечает прочитанным и не отвечает покупателю.
    #[tool(name = "wb_review", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_review(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbCustomerItemInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "feedbacks:/api/v1/feedback";
        let data = self
            .wb_client
            .review(&account, &input.id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Вопросы покупателей WB: одна страница. limit + offset не более 10000. Категория ключа Отзывы и вопросы.
    #[tool(name = "wb_questions", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_questions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbFeedbackReadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "feedbacks:/api/v1/questions";
        let data = self
            .wb_client
            .questions(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Один вопрос WB по ID без изменения его состояния.
    #[tool(name = "wb_question", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_question(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbCustomerItemInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "feedbacks:/api/v1/question";
        let data = self
            .wb_client
            .question(&account, &input.id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Архивные отзывы WB, одна страница; архив не означает наличие ответа продавца.
    #[tool(name = "wb_reviews_archive", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_reviews_archive(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbReviewArchiveInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "feedbacks:/api/v1/feedbacks/archive";
        let data = self
            .wb_client
            .archived_reviews(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Заявки покупателей на возврат за доступные WB последние 14 дней, одна страница до 200. Это не полный финансовый реестр возвратов. Категория ключа Возвраты покупателей.
    #[tool(name = "wb_return_claims", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_return_claims(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbClaimReadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "returns:/api/v1/claims";
        let data = self
            .wb_client
            .return_claims(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Ошибки создания и редактирования карточек WB: одна страница пакетов. Продолжайте по updatedAt + batchUUID до next=false. Категория ключа Контент.
    #[tool(name = "wb_product_card_errors", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_product_card_errors(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbCardErrorsInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "content:/content/v2/cards/error/list";
        let data = self
            .wb_client
            .card_errors(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Карточки WB в корзине: одна страница, курсор trashedAt + nmID. Только чтение, карточки не восстанавливает.
    #[tool(name = "wb_product_cards_trash", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_product_cards_trash(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbCardsTrashInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "content:/content/v2/get/cards/trash";
        let data = self
            .wb_client
            .cards_trash(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Официальные характеристики предмета WB: required, тип, единицы и допустимое число значений для проверки карточки.
    #[tool(
        name = "wb_subject_characteristics",
        annotations(read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_subject_characteristics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSubjectInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "content:/content/v2/object/charcs/{subjectId}";
        let data = self
            .wb_client
            .subject_characteristics(&account, input.subject_id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Поставки FBW на склады WB, одна страница с датами и статусами. Это не задания FBS. Категория ключа Поставки.
    #[tool(name = "wb_supplies", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_supplies(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSuppliesReadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "supplies:/api/v1/supplies";
        let data = self
            .wb_client
            .supplies(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Детали поставки FBW: склад, статус, приёмка и количества. `is_preorder_id` явно выбирает ID предварительного заказа.
    #[tool(name = "wb_supply", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_supply(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSupplyReadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "supplies:/api/v1/supplies/{ID}";
        let data = self
            .wb_client
            .supply(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Товары поставки FBW: одна страница, количества отгруженного, принятого и доступного к продаже товара.
    #[tool(name = "wb_supply_goods", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_supply_goods(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSupplyGoodsInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "supplies:/api/v1/supplies/{ID}/goods";
        let data = self
            .wb_client
            .supply_goods(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Упаковка поставки FBW по ID поставки. Предварительный заказ этим методом не поддерживается.
    #[tool(name = "wb_supply_packages", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_supply_packages(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSupplyPackagesInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "supplies:/api/v1/supplies/{ID}/package";
        let data = self
            .wb_client
            .supply_packages(&account, input.supply_id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Доступные лимиты создания карточек WB. Получение лимитов не покупает новые слоты.
    #[tool(name = "wb_product_card_limits", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_product_card_limits(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "content:/content/v2/cards/limits";
        let data = self
            .wb_client
            .card_limits(&account)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
    /// Проверяет заполнение одной страницы карточек WB: фото, название, описание, габариты, вес и баркоды. Отсутствующее поле — N/D. Это локальные проверки заполнения, не статус модерации WB; ошибки публикации и требования предмета доступны отдельными инструментами.
    #[tool(
        name = "wb_product_content_diagnostics",
        annotations(read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_product_content_diagnostics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbProductCardsInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let payload = wb_product_cards_payload(&input)?;
        let endpoint = "content:/content/v2/get/cards/list";
        let data = self
            .wb_client
            .product_cards(
                &account,
                input.locale.map(|l| l.as_str().to_owned()),
                payload,
            )
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        let diagnostics = super::super::wb_diagnostics::diagnostics(&data)?;
        Ok(Self::wb_result(account, endpoint, diagnostics))
    }
}
