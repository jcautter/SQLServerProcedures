from .sql_apuracao_pos import SQLApuracaoPos
from .sql_item_fatura_simplificado import SQLItemFaturaSimplificado
from .sql_item_fatura_base_fat_plano import SQLItemFaturaBaseFatPlano
from .sql_saldo_negativo import SQLSaldoNegativo
from .sql_outros_decrementos import SQLOutrosDecrementos

class SASProcedures(
    SQLApuracaoPos
    , SQLItemFaturaSimplificado
    , SQLItemFaturaBaseFatPlano
    , SQLSaldoNegativo
    , SQLOutrosDecrementos
):
    
    def __init__(self):
        pass