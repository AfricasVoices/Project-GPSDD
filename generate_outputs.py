import argparse
from copy import deepcopy
from functools import partial

from core_data_modules.analysis import analysis_utils, AnalysisConfiguration
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import IOUtils, TimeUtils

from configuration.code_schemes import CodeSchemes
from configuration.coding_plans import get_rqa_coding_plans, get_demog_coding_plans, get_follow_up_coding_plans, \
    BUNGOMA_S01_RQA_CODING_PLANS, KILIFI_S01_RQA_CODING_PLANS, KIAMBU_S01_RQA_CODING_PLANS
from src import LoadData, TranslateRapidProKeys, AutoCode, ProductionFile, \
    ApplyManualCodes, AnalysisFile, WSCorrection
from src.lib import PipelineConfiguration, MessageFilters

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the post-fetch phase of the pipeline")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_run_mode", help="whether to generate analysis files or not",
                        choices=["all-stages", "auto-code-only"])
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("raw_data_dir", metavar="raw-data-dir",
                        help="Path to a directory containing the raw data files exported by fetch_raw_data.py")
    parser.add_argument("prev_coded_dir_path", metavar="prev-coded-dir-path",
                        help="Directory containing Coda files generated by a previous run of this pipeline. "
                             "New data will be appended to these files.")

    parser.add_argument("auto_coding_json_output_path", metavar="auto-coding-json-output-path",
                        help="Path to a JSON file to write the TracedData associated with auto-coding stage of the pipeline")
    parser.add_argument("messages_json_output_path", metavar="messages-json-output-path",
                        help="Path to a JSONL file to write the TracedData associated with the messages analysis file")
    parser.add_argument("individuals_json_output_path", metavar="individuals-json-output-path",
                        help="Path to a JSONL file to write the TracedData associated with the individuals analysis file")
    parser.add_argument("icr_output_dir", metavar="icr-output-dir",
                        help="Directory to write CSV files to, each containing 200 messages and message ids for use " 
                             "in inter-code reliability evaluation"),
    parser.add_argument("coded_dir_path", metavar="coded-dir-path",
                        help="Directory to write coded Coda files to")
    parser.add_argument("csv_by_message_output_path", metavar="csv-by-message-output-path",
                        help="Analysis dataset where messages are the unit for analysis (i.e. one message per row)")
    parser.add_argument("csv_by_individual_output_path", metavar="csv-by-individual-output-path",
                        help="Analysis dataset where respondents are the unit for analysis (i.e. one respondent "
                             "per row, with all their messages joined into a single cell)")
    parser.add_argument("production_csv_output_path", metavar="production-csv-output-path",
                        help="Path to a CSV file to write raw message and demographic responses to, for use in "
                             "radio show production"),

    args = parser.parse_args()

    pipeline_run_mode = args.pipeline_run_mode
    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    raw_data_dir = args.raw_data_dir
    prev_coded_dir_path = args.prev_coded_dir_path

    auto_coding_json_output_path = args.auto_coding_json_output_path
    messages_json_output_path = args.messages_json_output_path
    individuals_json_output_path = args.individuals_json_output_path
    icr_output_dir = args.icr_output_dir
    coded_dir_path = args.coded_dir_path
    csv_by_message_output_path = args.csv_by_message_output_path
    csv_by_individual_output_path = args.csv_by_individual_output_path
    production_csv_output_path = args.production_csv_output_path

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    log.info("Loading the raw data...")
    data = LoadData.load_raw_data(user, raw_data_dir, pipeline_configuration)

    log.info("Translating Rapid Pro Keys...")
    data = TranslateRapidProKeys.translate_rapid_pro_keys(user, data, pipeline_configuration)

    if pipeline_configuration.move_ws_messages:
        log.info("Pre-filtering empty message objects...")
        # This is a performance optimisation to save execution time + memory when moving WS messages, by removing
        # the need to mark and process a high volume of empty message objects as 'NR' in WS correction.
        # Empty message objects represent flow runs where the participants never sent a message e.g. from an advert
        # flow run where we asked someone a question but didn't receive a response.
        data = MessageFilters.filter_empty_messages(data,
                                                    [plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS])

        log.info("Moving WS messages...")
        data = WSCorrection.move_wrong_scheme_messages(user, data, prev_coded_dir_path)
    else:
        log.info("Not moving WS messages (because the 'MoveWSMessages' key in the pipeline configuration "
                 "json was set to 'false')")

    log.info("Auto Coding...")
    data = AutoCode.auto_code(user, data, pipeline_configuration, icr_output_dir, coded_dir_path)

    log.info("Exporting production CSV...")
    data = ProductionFile.generate(data, production_csv_output_path)

    if pipeline_run_mode == "all-stages":
        log.info("Running post labelling pipeline stages...")

        log.info("Applying Manual Codes from Coda...")
        data = ApplyManualCodes.apply_manual_codes(user, data, prev_coded_dir_path)

        if pipeline_configuration.pipeline_name == "gpsdd_all_locations_s01_pipeline":
            # Up to this point the pipeline has run as normal, keeping data separate by location.
            # Now, we need to produce analysis files that cover all locations, which requires combining the datasets into
            # one. This is very difficult, because the pipeline assumed that there would be one raw field and one coda
            # dataset per field.
            # We now have to patch up this dataset:

            # 1. Check that there are no participants who have participated in multiple locations.
            # If they have, crash because this will require additional work to handle correctly.
            def coding_plans_to_analysis_configurations(coding_plans):
                analysis_configurations = []
                for plan in coding_plans:
                    ccs = plan.coding_configurations
                    for cc in ccs:
                        analysis_configurations.append(
                            AnalysisConfiguration(cc.analysis_file_key, plan.raw_field, cc.coded_field, cc.code_scheme)
                        )
                return analysis_configurations

            bungoma_rqa_participants = {td["uid"] for td in
                                        analysis_utils.filter_opt_ins(data, "uid",
                                                                      coding_plans_to_analysis_configurations(
                                                                          BUNGOMA_S01_RQA_CODING_PLANS))}
            kilifi_rqa_participants = {td["uid"] for td in
                                       analysis_utils.filter_opt_ins(data, "uid",
                                                                     coding_plans_to_analysis_configurations(
                                                                         KILIFI_S01_RQA_CODING_PLANS))}
            kiambu_rqa_participants = {td["uid"] for td in
                                       analysis_utils.filter_opt_ins(data, "uid",
                                                                     coding_plans_to_analysis_configurations(
                                                                         KIAMBU_S01_RQA_CODING_PLANS))}

            duplicate_participants = bungoma_rqa_participants.intersection(kilifi_rqa_participants) \
                .union(bungoma_rqa_participants.intersection(kiambu_rqa_participants)) \
                .union(kiambu_rqa_participants.intersection(kilifi_rqa_participants))

            if len(duplicate_participants) > 0:
                log.error(f"Detected participants who took part in multiple locations: {duplicate_participants}")

                #Filter out duplicate participants
                data = [td for td in data if td['uid'] not in duplicate_participants]

            # 2. Change the coding plans to refer to ones that contain unified field names and code schemes etc. rather
            # than per-location
            PipelineConfiguration.RQA_CODING_PLANS = get_rqa_coding_plans(pipeline_configuration.pipeline_name, True)
            PipelineConfiguration.DEMOG_CODING_PLANS = get_demog_coding_plans(pipeline_configuration.pipeline_name, True)
            PipelineConfiguration.FOLLOW_UP_CODING_PLANS = get_follow_up_coding_plans(pipeline_configuration.pipeline_name, True)
            PipelineConfiguration.SURVEY_CODING_PLANS = PipelineConfiguration.DEMOG_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS

            # 3. Patch up all the labels' scheme ids to refer to the relevant 'all_locations' scheme rather than to
            # the individual location schemes.
            rqa_scheme_remaps = {
                "kilifi_rqa_s01e01_coded": CodeSchemes.ALL_LOCATIONS_S01E01,
                "kiambu_rqa_s01e01_coded": CodeSchemes.ALL_LOCATIONS_S01E01,
                "bungoma_rqa_s01e01_coded": CodeSchemes.ALL_LOCATIONS_S01E01,

                "kilifi_rqa_s01e02_coded": CodeSchemes.ALL_LOCATIONS_S01E02,
                "kiambu_rqa_s01e02_coded": CodeSchemes.ALL_LOCATIONS_S01E02,
                "bungoma_rqa_s01e02_coded": CodeSchemes.ALL_LOCATIONS_S01E02,

                "kilifi_rqa_s01e03_coded": CodeSchemes.ALL_LOCATIONS_S01E03,
                "kiambu_rqa_s01e03_coded": CodeSchemes.ALL_LOCATIONS_S01E03,
                "bungoma_rqa_s01e03_coded": CodeSchemes.ALL_LOCATIONS_S01E03,

                "kilifi_rqa_s01e04_coded": CodeSchemes.ALL_LOCATIONS_S01E04,
                "kiambu_rqa_s01e04_coded": CodeSchemes.ALL_LOCATIONS_S01E04,
                "bungoma_rqa_s01e04_coded": CodeSchemes.ALL_LOCATIONS_S01E04,

                "kilifi_rqa_s01e05_coded": CodeSchemes.ALL_LOCATIONS_S01E05,
                "kiambu_rqa_s01e05_coded": CodeSchemes.ALL_LOCATIONS_S01E05,
                "bungoma_rqa_s01e05_coded": CodeSchemes.ALL_LOCATIONS_S01E05,

                "kilifi_rqa_s01e06_coded": CodeSchemes.ALL_LOCATIONS_S01E06,
                "kiambu_rqa_s01e06_coded": CodeSchemes.ALL_LOCATIONS_S01E06,
                "bungoma_rqa_s01e06_coded": CodeSchemes.ALL_LOCATIONS_S01E06,

                "kilifi_rqa_s01e07_coded": CodeSchemes.ALL_LOCATIONS_S01E07,
                "kiambu_rqa_s01e07_coded": CodeSchemes.ALL_LOCATIONS_S01E07,
                "bungoma_rqa_s01e07_coded": CodeSchemes.ALL_LOCATIONS_S01E07,

                "kilifi_rqa_s01e08_coded": CodeSchemes.ALL_LOCATIONS_S01E08,
                "kiambu_rqa_s01e08_coded": CodeSchemes.ALL_LOCATIONS_S01E08,
                "bungoma_rqa_s01e08_coded": CodeSchemes.ALL_LOCATIONS_S01E08,

                "kilifi_rqa_s01e09_coded": CodeSchemes.ALL_LOCATIONS_S01E09,
                "bungoma_rqa_s01e09_coded": CodeSchemes.ALL_LOCATIONS_S01E09,

                "kilifi_rqa_s01e10_coded": CodeSchemes.ALL_LOCATIONS_S01E10,
                "bungoma_rqa_s01e10_coded": CodeSchemes.ALL_LOCATIONS_S01E10,

                "kilifi_baseline_community_awareness_coded": CodeSchemes.ALL_LOCATIONS_BASELINE_COMMUNITY_AWARENESS,
                "kiambu_baseline_community_awareness_coded": CodeSchemes.ALL_LOCATIONS_BASELINE_COMMUNITY_AWARENESS,
                "bungoma_baseline_community_awareness_coded": CodeSchemes.ALL_LOCATIONS_BASELINE_COMMUNITY_AWARENESS,

                "kilifi_baseline_government_role_coded": CodeSchemes.ALL_LOCATIONS_BASELINE_GOVERNMENT_ROLE,
                "kiambu_baseline_government_role_coded": CodeSchemes.ALL_LOCATIONS_BASELINE_GOVERNMENT_ROLE,
                "bungoma_baseline_government_role_coded": CodeSchemes.ALL_LOCATIONS_BASELINE_GOVERNMENT_ROLE,

                "kilifi_endline_community_awareness_coded": CodeSchemes.ALL_LOCATIONS_ENDLINE_COMMUNITY_AWARENESS,
                "kiambu_endline_community_awareness_coded": CodeSchemes.ALL_LOCATIONS_ENDLINE_COMMUNITY_AWARENESS,
                "bungoma_endline_community_awareness_coded": CodeSchemes.ALL_LOCATIONS_ENDLINE_COMMUNITY_AWARENESS,

                "kilifi_endline_government_role_coded": CodeSchemes.ALL_LOCATIONS_ENDLINE_GOVERNMENT_ROLE,
                "kiambu_endline_government_role_coded": CodeSchemes.ALL_LOCATIONS_ENDLINE_GOVERNMENT_ROLE,
                "bungoma_endline_government_role_coded": CodeSchemes.ALL_LOCATIONS_ENDLINE_GOVERNMENT_ROLE
            }

            for td in data:
                remapped = dict()
                for field, target_scheme in rqa_scheme_remaps.items():
                    labels = deepcopy(td[field])
                    for l in labels:
                        l["SchemeID"] = target_scheme.scheme_id
                    remapped[field] = labels
                td.append_data(remapped, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

            # 4. Convert the fields in the bungoma/kilifi/kiambu coding plans relevant to analysis to their unified
            # fields. This is mostly like folding messages -> individuals, except we now have to account for some fields
            # being None, which is why there are some special case strategies implemented inline here.
            def assert_equal(x, y):
                if x is None:
                    return y
                if y is None:
                    return x

                assert x == y, f"{x} != {y}"
                return x

            def disabled_assert_equal(x, y):
                if x is None:
                    return y
                if y is None:
                    return x

                if x != y:
                    return x


            def demog_labels(code_scheme, x, y):
                if code_scheme.get_code_with_code_id(x["CodeID"]).control_code in ["NA", "NR"]:
                    return y
                if code_scheme.get_code_with_code_id(y["CodeID"]).control_code in ["NA", "NR"]:
                    return x

                assert x['CodeID'] == y['CodeID'], f"{x['CodeID']} == {y['CodeID']}"

                return x

            def disabled_demog_labels(code_scheme, x, y):
                if code_scheme.get_code_with_code_id(x["CodeID"]).control_code in ["NA", "NR"]:
                    return y
                if code_scheme.get_code_with_code_id(y["CodeID"]).control_code in ["NA", "NR"]:
                    return x

                return x

            remappings = {
                "rqa_s01e01_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E01),
                "rqa_s01e02_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E02),
                "rqa_s01e03_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E03),
                "rqa_s01e04_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E04),
                "rqa_s01e05_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E05),
                "rqa_s01e06_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E06),
                "rqa_s01e07_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E07),
                "rqa_s01e08_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E08),
                "rqa_s01e09_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E09),
                "rqa_s01e10_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_S01E10),

                "rqa_s01e01_raw": FoldStrategies.concatenate,
                "rqa_s01e02_raw": FoldStrategies.concatenate,
                "rqa_s01e03_raw": FoldStrategies.concatenate,
                "rqa_s01e04_raw": FoldStrategies.concatenate,
                "rqa_s01e05_raw": FoldStrategies.concatenate,
                "rqa_s01e06_raw": FoldStrategies.concatenate,
                "rqa_s01e09_raw": FoldStrategies.concatenate,
                "rqa_s01e10_raw": FoldStrategies.concatenate,

                "gender_coded": partial(demog_labels, CodeSchemes.GENDER),
                "age_coded": partial(disabled_demog_labels, CodeSchemes.AGE),
                "age_category_coded": partial(disabled_demog_labels, CodeSchemes.AGE_CATEGORY),
                "county_coded": partial(demog_labels, CodeSchemes.KENYA_COUNTY),
                "constituency_coded": partial(demog_labels, CodeSchemes.KENYA_CONSTITUENCY),
                "disabled_coded": partial(disabled_demog_labels, CodeSchemes.DISABLED),

                "gender_raw": disabled_assert_equal,
                "age_raw": disabled_assert_equal,
                "location_raw": disabled_assert_equal,
                "disabled_raw": disabled_assert_equal,

                "baseline_community_awareness_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_BASELINE_COMMUNITY_AWARENESS),
                "baseline_government_role_coded": partial(FoldStrategies.list_of_labels, CodeSchemes.ALL_LOCATIONS_BASELINE_GOVERNMENT_ROLE),

                "baseline_community_awareness_raw": assert_equal,
                "baseline_government_role_raw": assert_equal,

                "endline_community_awareness_coded": partial(FoldStrategies.list_of_labels,
                                                              CodeSchemes.ALL_LOCATIONS_ENDLINE_COMMUNITY_AWARENESS),
                "endline_government_role_coded": partial(FoldStrategies.list_of_labels,
                                                          CodeSchemes.ALL_LOCATIONS_ENDLINE_GOVERNMENT_ROLE),

                "endline_community_awareness_raw": assert_equal,
                "endline_government_role_raw": assert_equal
            }

            for td in data:
                remapped = dict()
                for field, strategy in remappings.items():
                    x = td.get(f"kilifi_{field}")
                    y = td.get(f"kiambu_{field}")
                    z = td.get(f"bungoma_{field}")

                    if y is not None:
                        folded = strategy(strategy(x, y), z)
                    else:
                        folded = strategy(x, z)

                    if folded is not None:
                        remapped[field] = folded

                td.append_data(remapped, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        log.info("Tagging listening group participants & Generating Analysis CSVs...")
        messages_data, individuals_data = AnalysisFile.generate(user, data, pipeline_configuration, raw_data_dir,
                                                                csv_by_message_output_path,
                                                                csv_by_individual_output_path)

        log.info("Writing messages TracedData to file...")
        IOUtils.ensure_dirs_exist_for_file(messages_json_output_path)
        with open(messages_json_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(messages_data, f)

        log.info("Writing individuals TracedData to file...")
        IOUtils.ensure_dirs_exist_for_file(individuals_json_output_path)
        with open(individuals_json_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(individuals_data, f)
    else:
        assert pipeline_run_mode == "auto-code-only", "pipeline run mode must be either auto-code-only or all-stages"
        log.info("Writing Auto-Coding TracedData to file...")
        IOUtils.ensure_dirs_exist_for_file(auto_coding_json_output_path)
        with open(auto_coding_json_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(data, f)

    log.info("Python script complete")
