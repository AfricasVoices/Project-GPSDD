from core_data_modules.cleaners import somali, swahili, Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies

from configuration import code_imputation_functions
from configuration.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingConfiguration, CodingModes, CodingPlan


def clean_age_with_range_filter(text):
    """
    Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
    """
    age = swahili.DemographicCleaner.clean_age(text)
    if type(age) == int and 10 <= age < 100:
        return str(age)
        # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
        #       NC in the case where age is an int but is out of range
    else:
        return Codes.NOT_CODED

KILIFI_S01_RQA_CODING_PLANS = [
    CodingPlan(raw_field="kilifi_rqa_s01e01_raw",
               listening_group_filename="gpsdd_kilifi_s01e01_listening_group.csv",
               time_field="sent_on",
               run_id_field="kilifi_rqa_s01e01_run_id",
               coda_filename="GPSDD_KILIFI_s01e01.json",
               icr_filename="kilifi_s01e01.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KILIFI_S01E01,
                       coded_field="rqa_s01e01_coded",
                       analysis_file_key="rqa_s01e01",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KILIFI_S01E01, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KILIFI s01e01"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kilifi_rqa_s01e02_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kilifi_s01e02_listening_group.csv",
               run_id_field="kilifi_rqa_s01e02_run_id",
               coda_filename="GPSDD_KILIFI_s01e02.json",
               icr_filename="kilifi_s01e02.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KILIFI_S01E02,
                       coded_field="rqa_s01e02_coded",
                       analysis_file_key="rqa_s01e02",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KILIFI_S01E02, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KILIFI s01e02"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kilifi_rqa_s01e03_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kilifi_s01e03_listening_group.csv",
               run_id_field="kilifi_rqa_s01e03_run_id",
               coda_filename="GPSDD_KILIFI_s01e03.json",
               icr_filename="kilifi_s01e03.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KILIFI_S01E03,
                       coded_field="rqa_s01e03_coded",
                       analysis_file_key="rqa_s01e03",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KILIFI_S01E03, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KILIFI s01e03"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kilifi_rqa_s01e04_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kilifi_s01e04_listening_group.csv",
               run_id_field="kilifi_rqa_s01e04_run_id",
               coda_filename="GPSDD_KILIFI_s01e04.json",
               icr_filename="kilifi_s01e04.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KILIFI_S01E04,
                       coded_field="rqa_s01e04_coded",
                       analysis_file_key="rqa_s01e04",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KILIFI_S01E04, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KILIFI s01e04"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kilifi_rqa_s01e05_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kilifi_s01e05_listening_group.csv",
               run_id_field="kilifi_rqa_s01e05_run_id",
               coda_filename="GPSDD_KILIFI_s01e05.json",
               icr_filename="kilifi_s01e05.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KILIFI_S01E05,
                       coded_field="rqa_s01e05_coded",
                       analysis_file_key="rqa_s01e05",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KILIFI_S01E05, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KILIFI s01e05"),
               raw_field_fold_strategy=FoldStrategies.concatenate)
]

KIAMBU_S01_RQA_CODING_PLANS = [
    CodingPlan(raw_field="kiambu_rqa_s01e01_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kiambu_s01e02_listening_group.csv",
               run_id_field="kiambu_rqa_s01e01_run_id",
               coda_filename="GPSDD_KIAMBU_s01e01.json",
               icr_filename="kiambu_s01e01.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KIAMBU_S01E01,
                       coded_field="rqa_s01e01_coded",
                       analysis_file_key="rqa_s01e01",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KIAMBU_S01E01, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KIAMBU s01e01"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kiambu_rqa_s01e02_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kiambu_s01e03_listening_group.csv",
               run_id_field="kiambu_rqa_s01e02_run_id",
               coda_filename="GPSDD_KIAMBU_s01e02.json",
               icr_filename="kiambu_s01e02.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KIAMBU_S01E02,
                       coded_field="rqa_s01e02_coded",
                       analysis_file_key="rqa_s01e02",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KIAMBU_S01E02, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KIAMBU s01e02"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kiambu_rqa_s01e03_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kiambu_s01e03_listening_group.csv",
               run_id_field="kiambu_rqa_s01e03_run_id",
               coda_filename="GPSDD_KIAMBU_s01e03.json",
               icr_filename="kiambu_s01e03.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KIAMBU_S01E03,
                       coded_field="rqa_s01e03_coded",
                       analysis_file_key="rqa_s01e03",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KIAMBU_S01E03, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KIAMBU s01e03"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kiambu_rqa_s01e04_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kiambu_s01e04_listening_group.csv",
               run_id_field="kiambu_rqa_s01e04_run_id",
               coda_filename="GPSDD_KIAMBU_s01e04.json",
               icr_filename="kiambu_s01e04.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KIAMBU_S01E05,
                       coded_field="rqa_s01e04_coded",
                       analysis_file_key="rqa_s01e04",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KIAMBU_S01E05, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KIAMBU s01e04"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="kiambu_rqa_s01e05_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_kiambu_s01e05_listening_group.csv",
               run_id_field="kiambu_rqa_s01e05_run_id",
               coda_filename="GPSDD_KIAMBU_s01e05.json",
               icr_filename="kiambu_s01e05.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KIAMBU_S01E05,
                       coded_field="rqa_s01e05_coded",
                       analysis_file_key="rqa_s01e05",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.KIAMBU_S01E05, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD KIAMBU s01e05"),
               raw_field_fold_strategy=FoldStrategies.concatenate)
]

BUNGOMA_S01_RQA_CODING_PLANS = [
    CodingPlan(raw_field="bungoma_rqa_s01e01_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_bungoma_s01e01_listening_group.csv",
               run_id_field="bungoma_rqa_s01e01_run_id",
               coda_filename="GPSDD_BUNGOMA_s01e01.json",
               icr_filename="bungoma_s01e01.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.BUNGOMA_S01E01,
                       coded_field="rqa_s01e01_coded",
                       analysis_file_key="rqa_s01e01",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.BUNGOMA_S01E01, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD BUNGOMA s01e01"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="bungoma_rqa_s01e02_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_bungoma_s01e02_listening_group.csv",
               run_id_field="bungoma_rqa_s01e02_run_id",
               coda_filename="GPSDD_BUNGOMA_s01e02.json",
               icr_filename="bungoma_s01e02.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.BUNGOMA_S01E02,
                       coded_field="rqa_s01e02_coded",
                       analysis_file_key="rqa_s01e02",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.BUNGOMA_S01E02, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD BUNGOMA s01e02"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="bungoma_rqa_s01e03_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_bungoma_s01e03_listening_group.csv",
               run_id_field="bungoma_rqa_s01e03_run_id",
               coda_filename="GPSDD_BUNGOMA_s01e03.json",
               icr_filename="bungoma_s01e03.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.BUNGOMA_S01E03,
                       coded_field="rqa_s01e03_coded",
                       analysis_file_key="rqa_s01e03",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.BUNGOMA_S01E03, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD BUNGOMA s01e03"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="bungoma_rqa_s01e04_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_bungoma_s01e04_listening_group.csv",
               run_id_field="bungoma_rqa_s01e04_run_id",
               coda_filename="GPSDD_BUNGOMA_s01e04.json",
               icr_filename="bungoma_s01e04.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.BUNGOMA_S01E04,
                       coded_field="rqa_s01e04_coded",
                       analysis_file_key="rqa_s01e04",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.BUNGOMA_S01E04, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD BUNGOMA s01e04"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="bungoma_rqa_s01e05_raw",
               time_field="sent_on",
               listening_group_filename="gpsdd_bungoma_s01e05_listening_group.csv",
               run_id_field="bungoma_rqa_s01e05_run_id",
               coda_filename="GPSDD_BUNGOMA_s01e05.json",
               icr_filename="bungoma_s01e05.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.BUNGOMA_S01E05,
                       coded_field="rqa_s01e05_coded",
                       analysis_file_key="rqa_s01e05",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.BUNGOMA_S01E05, x, y)
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("GPSDD BUNGOMA s01e05"),
               raw_field_fold_strategy=FoldStrategies.concatenate)
]

def get_rqa_coding_plans(pipeline_name):
    if pipeline_name == "gpsdd_kilifi_s01_pipeline":
        return KILIFI_S01_RQA_CODING_PLANS

    elif pipeline_name == "gpsdd_kiambu_s01_pipeline":
        return KIAMBU_S01_RQA_CODING_PLANS

    elif pipeline_name == "gpsdd_bungoma_s01_pipeline":
        return BUNGOMA_S01_RQA_CODING_PLANS

    elif pipeline_name == "gpsdd_all_locations_s01_pipeline":
        return KIAMBU_S01_RQA_CODING_PLANS + KILIFI_S01_RQA_CODING_PLANS + BUNGOMA_S01_RQA_CODING_PLANS


KILIFI_DEMOG_CODING_PLANS = [
    CodingPlan(raw_field="kilifi_gender_raw",
               time_field="kilifi_gender_time",
               coda_filename="GPSDD_KILIFI_gender.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.GENDER,
                       cleaner=somali.DemographicCleaner.clean_gender,
                       coded_field="gender_coded",
                       analysis_file_key="gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kilifi gender"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kilifi_age_raw",
               time_field="kilifi_age_time",
               coda_filename="GPSDD_KILIFI_age.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.AGE,
                       cleaner=clean_age_with_range_filter,
                       coded_field="age_coded",
                       analysis_file_key="age",
                       include_in_theme_distribution=Codes.FALSE,
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   ),
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.AGE_CATEGORY,
                       coded_field="age_category_coded",
                       analysis_file_key="age_category",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               code_imputation_function=code_imputation_functions.impute_age_category,
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kilifi age"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kilifi_location_raw",
               time_field="kilifi_location_time",
               coda_filename="GPSDD_KILIFI_location.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KENYA_COUNTY,
                       coded_field="county_coded",
                       analysis_file_key="county",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   ),
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KENYA_CONSTITUENCY,
                       coded_field="constituency_coded",
                       analysis_file_key="constituency",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               code_imputation_function=code_imputation_functions.impute_kenya_location_codes,
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kilifi location"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kilifi_disabled_raw",
               time_field="kilifi_disabled_time",
               coda_filename="GPSDD_KILIFI_disabled.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.DISABLED,
                       coded_field="disabled_coded",
                       analysis_file_key="disabled_gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kilifi disabled"),
               raw_field_fold_strategy=FoldStrategies.assert_equal)
]

KIAMBU_DEMOG_CODING_PLANS = [
    CodingPlan(raw_field="kiambu_gender_raw",
               time_field="kiambu_gender_time",
               coda_filename="GPSDD_KIAMBU_gender.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.GENDER,
                       cleaner=somali.DemographicCleaner.clean_gender,
                       coded_field="gender_coded",
                       analysis_file_key="gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kiambu gender"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kiambu_age_raw",
               time_field="kiambu_age_time",
               coda_filename="GPSDD_KIAMBU_age.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.AGE,
                       cleaner=clean_age_with_range_filter,
                       coded_field="age_coded",
                       analysis_file_key="age",
                       include_in_theme_distribution=Codes.FALSE,
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   ),
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.AGE_CATEGORY,
                       coded_field="age_category_coded",
                       analysis_file_key="age_category",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               code_imputation_function=code_imputation_functions.impute_age_category,
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kiambu age"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kiambu_location_raw",
               time_field="kiambu_location_time",
               coda_filename="GPSDD_KIAMBU_location.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KENYA_COUNTY,
                       coded_field="county_coded",
                       analysis_file_key="county",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   ),
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KENYA_CONSTITUENCY,
                       coded_field="constituency_coded",
                       analysis_file_key="constituency",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               code_imputation_function=code_imputation_functions.impute_kenya_location_codes,
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kiambu location"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kiambu_disabled_raw",
               time_field="kiambu_disabled_time",
               coda_filename="GPSDD_KIAMBU_disabled.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.DISABLED,
                       coded_field="disabled_coded",
                       analysis_file_key="disabled_gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("kiambu disabled"),
               raw_field_fold_strategy=FoldStrategies.assert_equal)
]

BUNGOMA_DEMOG_CODING_PLANS = [
    CodingPlan(raw_field="bungoma_gender_raw",
               time_field="bungoma_gender_time",
               coda_filename="GPSDD_BUNGOMA_gender.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.GENDER,
                       cleaner=somali.DemographicCleaner.clean_gender,
                       coded_field="gender_coded",
                       analysis_file_key="gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("bungoma gender"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="bungoma_age_raw",
               time_field="bungoma_age_time",
               coda_filename="GPSDD_BUNGOMA_age.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.AGE,
                       cleaner=clean_age_with_range_filter,
                       coded_field="age_coded",
                       analysis_file_key="age",
                       include_in_theme_distribution=Codes.FALSE,
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   ),
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.AGE_CATEGORY,
                       coded_field="age_category_coded",
                       analysis_file_key="age_category",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               code_imputation_function=code_imputation_functions.impute_age_category,
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("bungoma age"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="bungoma_location_raw",
               time_field="bungoma_location_time",
               coda_filename="GPSDD_BUNGOMA_location.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KENYA_COUNTY,
                       coded_field="county_coded",
                       analysis_file_key="county",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   ),
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KENYA_CONSTITUENCY,
                       coded_field="constituency_coded",
                       analysis_file_key="constituency",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               code_imputation_function=code_imputation_functions.impute_kenya_location_codes,
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("bungoma location"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="bungoma_disabled_raw",
               time_field="bungoma_disabled_time",
               coda_filename="GPSDD_BUNGOMA_disabled.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.DISABLED,
                       coded_field="disabled_coded",
                       analysis_file_key="disabled_gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal
                   )
               ],
               ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("bungoma disabled"),
               raw_field_fold_strategy=FoldStrategies.assert_equal)
]

def get_demog_coding_plans(pipeline_name):
    if pipeline_name == "gpsdd_kilifi_s01_pipeline":
        return KILIFI_DEMOG_CODING_PLANS

    elif pipeline_name == "gpsdd_kiambu_s01_pipeline":
        return KIAMBU_DEMOG_CODING_PLANS

    elif pipeline_name == "gpsdd_bungoma_s01_pipeline":
        return BUNGOMA_DEMOG_CODING_PLANS

    elif pipeline_name == "gpsdd_all_locations_s01_pipeline":
        return KILIFI_DEMOG_CODING_PLANS + KIAMBU_DEMOG_CODING_PLANS + BUNGOMA_DEMOG_CODING_PLANS


def get_follow_up_coding_plans(pipeline_name):
    return []


def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.WS_CORRECT_DATASET
