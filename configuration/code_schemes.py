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
    KILIFI_S01E06 = _open_scheme("kilifi_s01e06.json")
    KILIFI_S01E07 = _open_scheme("kilifi_s01e07.json")
    KILIFI_S01E08 = _open_scheme("kilifi_s01e08.json")
    KILIFI_S01_NOISE_HANDLER = _open_scheme("kilifi_s01_noise_handler.json")
    KILIFI_S01E09 = _open_scheme("kilifi_s01e09.json")
    KILIFI_S01E10 = _open_scheme("kilifi_s01e10.json")

    KIAMBU_S01E01 = _open_scheme("kiambu_s01e01.json")
    KIAMBU_S01E02 = _open_scheme("kiambu_s01e02.json")
    KIAMBU_S01E03 = _open_scheme("kiambu_s01e03.json")
    KIAMBU_S01E04 = _open_scheme("kiambu_s01e04.json")
    KIAMBU_S01E05 = _open_scheme("kiambu_s01e05.json")
    KIAMBU_S01E06 = _open_scheme("kiambu_s01e06.json")
    KIAMBU_S01E07 = _open_scheme("kiambu_s01e07.json")
    KIAMBU_S01E08 = _open_scheme("kiambu_s01e08.json")
    ICRAF_CONSENT = _open_scheme("icraf_consent.json")

    BUNGOMA_S01E01 = _open_scheme("bungoma_s01e01.json")
    BUNGOMA_S01E02 = _open_scheme("bungoma_s01e02.json")
    BUNGOMA_S01E03 = _open_scheme("bungoma_s01e03.json")
    BUNGOMA_S01E04 = _open_scheme("bungoma_s01e04.json")
    BUNGOMA_S01E05 = _open_scheme("bungoma_s01e05.json")
    BUNGOMA_S01E06 = _open_scheme("bungoma_s01e06.json")
    BUNGOMA_S01E07 = _open_scheme("bungoma_s01e07.json")
    BUNGOMA_S01E08 = _open_scheme("bungoma_s01e08.json")
    BUNGOMA_S01E09 = _open_scheme("bungoma_s01e09.json")
    BUNGOMA_S01E10 = _open_scheme("bungoma_s01e10.json")

    ALL_LOCATIONS_S01E01 = _open_scheme("all_locations_s01e01.json")
    ALL_LOCATIONS_S01E02 = _open_scheme("all_locations_s01e02.json")
    ALL_LOCATIONS_S01E03 = _open_scheme("all_locations_s01e03.json")
    ALL_LOCATIONS_S01E04 = _open_scheme("all_locations_s01e04.json")
    ALL_LOCATIONS_S01E05 = _open_scheme("all_locations_s01e05.json")
    ALL_LOCATIONS_S01E06 = _open_scheme("all_locations_s01e06.json")
    ALL_LOCATIONS_S01E07 = _open_scheme("all_locations_s01e07.json")
    ALL_LOCATIONS_S01E08 = _open_scheme("all_locations_s01e08.json")
    ALL_LOCATIONS_S01E09 = _open_scheme("all_locations_s01e09.json")
    ALL_LOCATIONS_S01E10 = _open_scheme("all_locations_s01e10.json")

    KENYA_CONSTITUENCY = _open_scheme("kenya_constituency.json")
    KENYA_COUNTY = _open_scheme("kenya_county.json")
    GENDER = _open_scheme("gender.json")
    DISABLED = _open_scheme("disabled.json")
    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")

    KIAMBU_BASELINE_COMMUNITY_AWARENESS = _open_scheme("kiambu_baseline_community_awareness.json")
    KIAMBU_BASELINE_GOVERNMENT_ROLE = _open_scheme("kiambu_baseline_government_role.json")

    KILIFI_BASELINE_COMMUNITY_AWARENESS = _open_scheme("kilifi_baseline_community_awareness.json")
    KILIFI_BASELINE_GOVERNMENT_ROLE = _open_scheme("kilifi_baseline_government_role.json")

    BUNGOMA_BASELINE_COMMUNITY_AWARENESS = _open_scheme("bungoma_baseline_community_awareness.json")
    BUNGOMA_BASELINE_GOVERNMENT_ROLE = _open_scheme("bungoma_baseline_government_role.json")

    ALL_LOCATIONS_BASELINE_COMMUNITY_AWARENESS = _open_scheme("all_locations_baseline_community_awareness.json")
    ALL_LOCATIONS_BASELINE_GOVERNMENT_ROLE = _open_scheme("all_locations_baseline_government_role.json")

    KIAMBU_ENDLINE_COMMUNITY_AWARENESS = _open_scheme("kiambu_endline_community_awareness.json")
    KIAMBU_ENDLINE_GOVERNMENT_ROLE = _open_scheme("kiambu_endline_government_role.json")

    KILIFI_ENDLINE_COMMUNITY_AWARENESS = _open_scheme("kilifi_endline_community_awareness.json")
    KILIFI_ENDLINE_GOVERNMENT_ROLE = _open_scheme("kilifi_endline_government_role.json")

    BUNGOMA_ENDLINE_COMMUNITY_AWARENESS = _open_scheme("bungoma_endline_community_awareness.json")
    BUNGOMA_ENDLINE_GOVERNMENT_ROLE = _open_scheme("bungoma_endline_government_role.json")

    ALL_LOCATIONS_ENDLINE_COMMUNITY_AWARENESS = _open_scheme("all_locations_endline_community_awareness.json")
    ALL_LOCATIONS_ENDLINE_GOVERNMENT_ROLE = _open_scheme("all_locations_endline_government_role.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")
