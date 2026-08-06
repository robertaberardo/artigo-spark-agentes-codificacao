"""Régua de comparação numérica adaptativa — o ponto mais sensível do score.

A tolerância deriva das casas decimais que o candidato **efetivamente gravou**, e a
detecção precisa ser feita sobre o valor gravado, não sobre a representação em ponto
flutuante: contar casas de um ``double`` ingenuamente devolveria dezessete. Um erro
aqui contamina a métrica principal do estudo, daí o teste dedicado com valores
construídos.

Cobre também a propriedade que o comparador registrado **não** tinha: monotonicidade
na precisão. Candidato mais preciso nunca pode reprovar onde um menos preciso passa.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..",
                                                "analysis")))
import score_compare as rc  # noqa: E402


# --- detecção de casas decimais -------------------------------------------------------

def test_detecta_casas_por_valor_gravado():
    assert rc.detectar_casas([1.5, 2.5, 3.0]) == 1
    assert rc.detectar_casas([1.23, 4.56]) == 2
    assert rc.detectar_casas([1.234, 5.678]) == 3
    assert rc.detectar_casas([0.0, 12.0]) == 0


def test_precisao_plena_nao_e_confundida_com_casas_baixas():
    """Valor de precisão plena não pode ser lido como poucas casas."""
    assert rc.detectar_casas([1.2345678901234, 2.7182818284590]) >= 13


def test_coluna_toda_nula_devolve_indefinido():
    assert rc.detectar_casas([None, None]) is None


def test_nulos_nao_atrapalham_a_deteccao():
    assert rc.detectar_casas([None, 1.25, None, 3.75]) == 2


# --- tolerância ------------------------------------------------------------------------

def test_tolerancia_por_casas():
    assert rc.tolerancia_adaptativa(1) == 0.05
    assert rc.tolerancia_adaptativa(2) == 0.005
    assert rc.tolerancia_adaptativa(3) == 0.0005


def test_piso_de_ruido_de_ponto_flutuante():
    """Abaixo de 10^-6 a tolerância não encolhe: é o piso de ruído."""
    assert rc.tolerancia_adaptativa(12) == 1e-6
    assert rc.tolerancia_adaptativa(None) == 1e-6


# --- comparação ------------------------------------------------------------------------

def test_arredondamento_correto_passa():
    """O valor gravado é a referência corretamente arredondada na precisão escolhida."""
    assert rc.compara_valor(1.23, 1.2349, "adaptativo", 2) is True
    assert rc.compara_valor(1.235, 1.23456, "adaptativo", 3) is True


def test_erro_real_de_calculo_continua_reprovando():
    """Não é passe livre: erro real excede em muito o passo de arredondamento."""
    assert rc.compara_valor(1.23, 1.98, "adaptativo", 2) is False
    assert rc.compara_valor(10.0, 11.0, "adaptativo", 1) is False


def test_null_safe():
    assert rc.compara_valor(None, None, "adaptativo", 2) is True
    assert rc.compara_valor(0.0, None, "adaptativo", 2) is False
    assert rc.compara_valor(None, 0.0, "adaptativo", 2) is False


def test_monotonicidade_na_precisao():
    """Candidato mais preciso nunca reprova onde um menos preciso passa.

    É a propriedade que o comparador registrado não tinha: ele comparava igualdade
    após arredondar os dois lados, de modo que gravar três casas reprovava onde duas
    passavam.
    """
    referencia = 1.2249
    grosso = round(referencia, 2)   # 1.22
    fino = round(referencia, 3)     # 1.225
    pleno = referencia

    assert rc.compara_valor(grosso, referencia, "adaptativo", 2) is True
    assert rc.compara_valor(fino, referencia, "adaptativo", 3) is True
    assert rc.compara_valor(pleno, referencia, "adaptativo", 15) is True

    # E o defeito do critério registrado, demonstrado: o mais preciso reprovava.
    assert rc.compara_valor(grosso, referencia, "registrado", 2) is True
    assert rc.compara_valor(fino, referencia, "registrado", 3) is False


def test_predicado_registrado_usa_half_up_como_o_spark():
    """Reproduzir o oráculo exige HALF_UP, não o arredondamento bancário do Python.

    Com HALF_EVEN, ``round(0.125, 2)`` devolve 0.12 e a verificação de invariância
    acusaria divergência que é do comparador, não do dado.
    """
    assert rc.round_half_up(0.125, 2) == 0.13
    assert rc.round_half_up(0.135, 2) == 0.14
    assert round(0.125, 2) == 0.12  # comportamento do embutido, para contraste
