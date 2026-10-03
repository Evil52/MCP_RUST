use super::ProblemKind;

pub(super) const fn problem_text(kind: ProblemKind) -> (&'static str, &'static str) {
    match kind {
        ProblemKind::WbFbwStockoutWithAdSpend => (
            "Нулевой остаток FBW и расходы рекламы за период",
            "Проверить FBS, доставку и текущий состав кампании",
        ),
        ProblemKind::WbFbwStockout => (
            "Нулевой остаток на складах WB (FBW)",
            "Проверить FBS и пополнение WB",
        ),
        ProblemKind::WbFbwLowStockCover => (
            "Низкий запас на складах WB (FBW)",
            "Проверить FBS, доставку и пополнение WB",
        ),
        ProblemKind::AdvertisedWithoutStock => (
            "Реклама при нулевом остатке",
            "Проверить остаток и кампанию",
        ),
        ProblemKind::Stockout => ("Товар закончился", "Запланировать пополнение"),
        ProblemKind::LowStockCover => ("Низкий запас", "Уточнить поставку"),
        ProblemKind::SpendWithoutOrders => (
            "Расход без атрибутированных заказов",
            "Проверить запросы, карточку и ставку",
        ),
        ProblemKind::HighDrr => ("Высокий ДРР", "Проверить кампанию до изменения ставки"),
    }
}
