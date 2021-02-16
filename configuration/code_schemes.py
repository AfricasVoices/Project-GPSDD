import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    KILIFI_S01E01 = _open_scheme("kilifi_s01e01.json")
    KILIFI_S01E02 = _open_scheme("kilifi_s01e02.json")
    KILIFI_S01E03 = _open_scheme("kilifi_s01e03.json")
    KILIFI_S01E04 = _open_scheme("kilifi_s01e04.json")
    KILIFI_S01E05 = _open_scheme("kilifi_s01e05.json")

    KIAMBU_S01E01 = _open_scheme("kiambu_s01e01.json")
    KIAMBU_S01E02 = _open_scheme("kiambu_s01e02.json")
    KIAMBU_S01E03 = _open_scheme("kiambu_s01e03.json")
    KIAMBU_S01E04 = _open_scheme("kiambu_s01e04.json")
    KIAMBU_S01E05 = _open_scheme("kiambu_s01e05.json")

    BUNGOMA_S01E01 = _open_scheme("bungoma_s01e01.json")
    BUNGOMA_S01E02 = _open_scheme("bungoma_s01e02.json")
    BUNGOMA_S01E03 = _open_scheme("bungoma_s01e03.json")
    BUNGOMA_S01E04 = _open_scheme("bungoma_s01e04.json")
    BUNGOMA_S01E05 = _open_scheme("bungoma_s01e05.json")

    KENYA_CONSTITUENCY = _open_scheme("kenya_constituency.json")
    KENYA_COUNTY = _open_scheme("kenya_county.json")
    GENDER = _open_scheme("gender.json")
    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")
