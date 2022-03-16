from spyql.nulltype import *
from spyql.sqlfuncs import *
from spyql.qdict import qdict
import numpy as np
import math


def test_arithmetic_operators():
    assert NULL + 2 is NULL
    assert 2 + NULL is NULL
    assert NULL + NULL is NULL
    assert 1 + 2 is not NULL
    assert 2 + 3 + NULL is NULL

    assert NULL - 2 is NULL
    assert 2 - NULL is NULL
    assert NULL - NULL is NULL
    assert 1 - 2 is not NULL
    assert 2 - 3 - NULL is NULL

    assert NULL * 2 is NULL
    assert NULL * 0 is NULL
    assert 2 * NULL is NULL
    assert NULL * NULL is NULL
    assert 1 * 2 is not NULL
    assert 2 * 3 * NULL is NULL

    assert NULL / 2.0 is NULL
    assert NULL / 0.0 is NULL
    assert 2.0 / NULL is NULL
    assert NULL / NULL is NULL
    assert 1.0 / 2.0 is not NULL
    assert 2.0 / 3.0 / NULL is NULL

    assert NULL // 2 is NULL
    assert NULL // 0 is NULL
    assert 2 // NULL is NULL
    assert NULL // NULL is NULL
    assert 1 // 2 is not NULL
    assert 2 // 3 // NULL is NULL

    assert NULL % 2 is NULL
    assert NULL % 0 is NULL
    assert 2 % NULL is NULL
    assert NULL % NULL is NULL
    assert 1 % 2 is not NULL
    assert 2 % 3 % NULL is NULL

    assert NULL ** 2 is NULL
    assert NULL ** 0 is NULL
    assert 2 ** NULL is NULL
    assert NULL ** NULL is NULL
    assert 2 ** 3 is not NULL
    assert 2 ** 3 ** NULL is NULL

    assert -NULL is NULL
    assert +NULL is NULL
    assert abs(NULL) is NULL
    assert round(NULL) is NULL
    assert math.trunc(NULL) is NULL
    assert math.ceil(NULL) is NULL
    assert math.floor(NULL) is NULL


def test_bitwise_operators():
    assert NULL << 2 is NULL
    assert NULL << 0 is NULL
    assert 2 << NULL is NULL
    assert NULL << NULL is NULL
    assert 100 << 3 is not NULL
    assert 100 << 3 << NULL is NULL

    assert NULL >> 2 is NULL
    assert NULL >> 0 is NULL
    assert 2 >> NULL is NULL
    assert NULL >> NULL is NULL
    assert 100 >> 3 is not NULL
    assert 100 >> 3 >> NULL is NULL

    assert NULL & 2 is NULL
    assert NULL & 0 is NULL
    assert 2 & NULL is NULL
    assert NULL & NULL is NULL
    assert 100 & 3 is not NULL
    assert 100 & 3 & NULL is NULL

    assert NULL | 2 is NULL
    assert NULL | 0 is NULL
    assert 2 | NULL is NULL
    assert NULL | NULL is NULL
    assert 100 | 3 is not NULL
    assert 100 | 3 | NULL is NULL

    assert NULL ^ 2 is NULL
    assert NULL ^ 0 is NULL
    assert 2 ^ NULL is NULL
    assert NULL ^ NULL is NULL
    assert 100 ^ 3 is not NULL
    assert 100 ^ 3 ^ NULL is NULL

    assert ~NULL is NULL
    assert ~~NULL is NULL


def test_bool_operators():
    assert not NULL  # NULL bool value is False

    assert (NULL and True) is NULL
    assert (True and NULL) is NULL

    assert (NULL and False) is NULL
    assert (
        False and NULL
    ) is False  # inconsistent behaviour... (despite NULL evals to False)

    assert (NULL and NULL) is NULL

    assert (NULL or True) is True
    assert (True or NULL) is True

    assert (NULL or False) is False
    assert (
        False or NULL
    ) is NULL  # inconsistent behaviour... (despite NULL evals to False)

    assert (NULL or NULL) is NULL


def test_comparison_operators():
    assert (NULL == 2) is NULL
    assert (2 == NULL) is NULL
    assert (NULL == NULL) is NULL

    assert (NULL != 2) is NULL
    assert (2 != NULL) is NULL
    assert (NULL != NULL) is NULL

    assert (NULL > 2) is NULL
    assert (2 > NULL) is NULL
    assert (NULL > NULL) is NULL

    assert (NULL >= 2) is NULL
    assert (2 >= NULL) is NULL
    assert (NULL >= NULL) is NULL

    assert (NULL < 2) is NULL
    assert (2 < NULL) is NULL
    assert (NULL < NULL) is NULL

    assert (NULL <= 2) is NULL
    assert (2 <= NULL) is NULL
    assert (NULL <= NULL) is NULL


def test_container_operators():
    assert len(NULL) == 0
    assert len([x for x in NULL]) == 0
    assert len([x for x in reversed(NULL)]) == 0
    assert NULL["abc"] is NULL
    assert NULL[1] is NULL
    assert NULL.get("abc") is NULL
    assert NULL.get("abc", None) is NULL
    assert not (1 in NULL)
    assert not ("a" in NULL)
    assert not (NULL in NULL)
    assert NULL in [NULL]


def test_matrix_mult():
    assert NULL @ np.array([1, 2]) is NULL
    assert NULL @ np.array([]) is NULL
    assert np.array([1, 2]) @ NULL is NULL
    assert NULL @ NULL is NULL
    assert np.array([1, 2]) @ np.array([3, 4]) is not NULL
    assert np.array([1, 2]) @ np.array([3, 4]) @ NULL is NULL


def test_casting():
    assert float_("1.1") == float("1.1")
    assert float_("-1.21e-5") == float("-1.21e-5")
    assert float_(1) == float(1)
    assert float_("") is NULL
    assert float_("abc") is NULL
    assert float_(NULL) is NULL

    assert int_("1") == int("1")
    assert int_("-981") == int("-981")
    assert int_(1) == int(1)
    assert int_("") is NULL
    assert int_("abc") is NULL
    assert int_(NULL) is NULL

    assert str_("1") == str("1")
    assert str_(1) == str(1)
    assert str_(-981) == str(-981)
    assert str_(-1.21e-5) == str(-1.21e-5)
    assert str_(NULL) is NULL

    assert complex_("1.1") == complex("1.1")
    assert complex_("1.1+2j") == complex("1.1+2j")
    assert complex_(1) == complex(1)
    assert complex_("") is NULL
    assert complex_("abc") is NULL
    assert complex_(NULL) is NULL


def test_aux_functions():
    assert coalesce(1, 2) == 1
    assert coalesce(NULL, 2) == 2
    assert ifnull(1, 2) == 1
    assert ifnull(NULL, 2) == 2
    assert nullif(1, 1) is NULL
    assert nullif(1, 2) == 1


def test_misc():
    assert null is NULL
    assert Null is NULL


def test_dict():
    qdict({})["abc"] is NULL
    qdict({"abc": None})["abc"] is NULL
    qdict({"abc": None})["abc"] is not None  # Attention!
    qdict({"abc": 1})["abc"] is not NULL
    qdict({"abc": "def"})["abc"] is not NULL
    qdict({})["abc"]["def"]["hij"] is NULL
    qdict({"abc": {"def": 1}})["abc"]["def"] is not NULL
