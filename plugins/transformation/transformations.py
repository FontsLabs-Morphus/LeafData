import json
import logging
import re
import sys
import typing as t
from pathlib import Path

from exceptions.custom_exception import MorphusAirflowException

sys.path.insert(0, str(Path(__file__).parent.parent))
from custom_operators.constants import NT_TRANSFORMATIONS
from transformation.transformations_expression import TransformationExpression

DEFAULT_EMAIL_ADDRESS_FILE_PATH = r"/opt/airflow/plugins/custom_operators/died_objects/replacement_object_email_address.txt"
DEFAULT_FACILITY_NAME_FILE_PATH = r"/opt/airflow/plugins/custom_operators/died_objects/replacement_object_facility_name.txt"
DEFAULT_FIRST_NAME_FILE_PATH = r"/opt/airflow/plugins/custom_operators/died_objects/replacement_object_first_name.txt"
DEFAULT_FULL_NAME_FILE_PATH = r"/opt/airflow/plugins/custom_operators/died_objects/replacement_object_full_name.txt"
DEFAULT_LAST_NAME_FILE_PATH = r"/opt/airflow/plugins/custom_operators/died_objects/replacement_object_last_name.txt"

DEID_JSON_DIR = r"/opt/airflow/plugins/custom_operators/died_objects"

ENTITY_CONFIGS = {
    'first_name': {
        'file_path': DEFAULT_FIRST_NAME_FILE_PATH,
        'subdir': 'first_name_maps',
        'pool_attr': '_first_names',
        'map_attr': '_first_name_map'
    },
    'last_name': {
        'file_path': DEFAULT_LAST_NAME_FILE_PATH,
        'subdir': 'last_name_maps',
        'pool_attr': '_last_names',
        'map_attr': '_last_name_map'
    },
    'full_name': {
        'file_path': DEFAULT_FULL_NAME_FILE_PATH,
        'subdir': 'full_name_maps',
        'pool_attr': '_full_names',
        'map_attr': '_full_name_map'
    },
    'email_address': {
        'file_path': DEFAULT_EMAIL_ADDRESS_FILE_PATH,
        'subdir': 'email_address_maps',
        'pool_attr': '_email_addresses',
        'map_attr': '_email_address_map'
    },
    'facility_name': {
        'file_path': DEFAULT_FACILITY_NAME_FILE_PATH,
        'subdir': 'facility_name_maps',
        'pool_attr': '_facility_names',
        'map_attr': '_facility_name_map'
    }
}


class TxFunctionMixins:

    # -------------------- SIMPLE TRANSFORMS --------------------
    def LowercaseToUppercase(self, val: str) -> str:
        try:
            if not isinstance(val, (str, type(None))):
                raise TypeError("Invalid type for val")
            return val.upper() if val else val
        except Exception as e:
            raise MorphusAirflowException(
                message="LowercaseToUppercase failed",
                error_source="TxFunctionMixins.LowercaseToUppercase",
                original_exception=e
            ) from e

    def UppercaseToLowercase(self, val: str) -> str:
        try:
            if not isinstance(val, (str, type(None))):
                raise TypeError("Invalid type for val")
            return val.lower() if val else val
        except Exception as e:
            raise MorphusAirflowException(
                message="UppercaseToLowercase failed",
                error_source="TxFunctionMixins.UppercaseToLowercase",
                original_exception=e
            ) from e

    def RemoveSpaces(self, val: str) -> str:
        try:
            if not isinstance(val, (str, type(None))):
                raise TypeError("Invalid type for val")
            return val.replace(" ", "") if val else val
        except Exception as e:
            raise MorphusAirflowException(
                message="RemoveSpaces failed",
                error_source="TxFunctionMixins.RemoveSpaces",
                original_exception=e
            ) from e

    def StringToInt(self, val: str) -> int:
        try:
            if not isinstance(val, (str, type(None))):
                raise TypeError("Invalid type for val")
            if not val:
                return None
            return int(val)
        except Exception as e:
            raise MorphusAirflowException(
                message=f"StringToInt failed for value '{val}'",
                error_source="TxFunctionMixins.StringToInt",
                original_exception=e
            ) from e

    # -------------------- DE-IDENTIFICATION --------------------
    def DeidentificationFirstName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, "first_name")

    def DeidentificationLastName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, "last_name")

    def DeidentificationFullName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, "full_name")

    def DeidentificationEmailAddress(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, "email_address")

    def DeidentificationFacilityName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, "facility_name")

    def _deidentify_entity(self, val: str, tid: str, tagID: str, entity_type: str) -> str:
        try:
            if not isinstance(val, (str, type(None))):
                raise TypeError("Invalid type for val")
            if not isinstance(tid, str) or not tid.strip():
                raise TypeError("Invalid type for tid")
            if not isinstance(tagID, str) or not tagID.strip():
                raise TypeError("Invalid type for tagID")
            if entity_type not in ENTITY_CONFIGS:
                raise ValueError(f"Unsupported entity type: {entity_type}")
            if not val:
                return val

            self._ensure_entity_pool(entity_type)
            path = self._tid_json_path(tid, tagID, entity_type)
            existing = self._load_tid_mapping(path)

            config = ENTITY_CONFIGS[entity_type]
            memory_map_attr = f"{config['map_attr']}_{tid}_{tagID}"
            memory_map = getattr(self, memory_map_attr, {})

            if existing is not None:
                if val in existing:
                    mapped = existing[val]
                    memory_map[val] = mapped
                    setattr(self, memory_map_attr, memory_map)
                    return mapped

                replacement = self._get_next_replacement(entity_type)
                existing[val] = replacement
                memory_map[val] = replacement
                setattr(self, memory_map_attr, memory_map)
                self._save_tid_mapping(path, existing)
                return replacement

            replacement = memory_map.get(val) or self._get_next_replacement(entity_type)
            memory_map[val] = replacement
            setattr(self, memory_map_attr, memory_map)
            self._save_tid_mapping(path, {val: replacement})
            return replacement

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="De-identification failed",
                error_source="TxFunctionMixins._deidentify_entity",
                original_exception=e
            ) from e

    # -------------------- HELPERS --------------------
    def _tid_json_path(self, tid: str, tagID: str, entity_type: str) -> str:
        try:
            if entity_type not in ENTITY_CONFIGS:
                raise ValueError(f"Unsupported entity type: {entity_type}")

            base_dir = getattr(self, "deid_json_dir", DEID_JSON_DIR)
            base_dir = Path(base_dir)

            config = ENTITY_CONFIGS[entity_type]
            directory = base_dir / config["subdir"] / self._sanitize_filename(tid)
            directory.mkdir(parents=True, exist_ok=True)

            safe_tagID = self._sanitize_filename(tagID)
            if not safe_tagID:
                raise ValueError("tagID resulted in empty filename")

            return str(directory / f"{safe_tagID}.json")

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to resolve TID JSON path",
                error_source="TxFunctionMixins._tid_json_path",
                original_exception=e
            ) from e

    def _sanitize_filename(self, filename: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", filename)

    def _load_tid_mapping(self, path: str) -> t.Optional[t.Dict[str, str]]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to load TID mapping",
                error_source="TxFunctionMixins._load_tid_mapping",
                original_exception=e
            ) from e

    def _save_tid_mapping(self, path: str, mapping: t.Dict[str, str]) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise MorphusAirflowException(
                message=f"Failed to save mapping to {path}",
                error_source="TxFunctionMixins._save_tid_mapping",
                original_exception=e
            ) from e

    def _get_next_replacement(self, entity_type: str) -> str:
        try:
            if entity_type not in ENTITY_CONFIGS:
                raise ValueError(f"Unsupported entity type: {entity_type}")

            pool_attr = ENTITY_CONFIGS[entity_type]["pool_attr"]
            pool = getattr(self, pool_attr, None)

            if not pool:
                raise RuntimeError(f"Ran out of replacement {entity_type} values")

            return pool.pop(0)

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to fetch next replacement value",
                error_source="TxFunctionMixins._get_next_replacement",
                original_exception=e
            ) from e

    def _ensure_entity_pool(self, entity_type: str) -> None:
        try:
            if entity_type not in ENTITY_CONFIGS:
                raise ValueError(f"Unsupported entity type: {entity_type}")

            config = ENTITY_CONFIGS[entity_type]
            pool_attr = config["pool_attr"]

            if not hasattr(self, pool_attr):
                file_path = getattr(self, f"{entity_type}_file_path", config["file_path"])
                pool = self._load_replacement_values(file_path)
                setattr(self, pool_attr, pool)

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to initialize entity replacement pool",
                error_source="TxFunctionMixins._ensure_entity_pool",
                original_exception=e
            ) from e

    @staticmethod
    def _load_replacement_values(path: str) -> t.List[str]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            if not lines:
                raise ValueError("Replacement file is empty")

            values = [line.strip() for line in lines[1:] if line.strip()]
            if not values:
                raise ValueError("No replacement values found")

            return values

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to load replacement values",
                error_source="TxFunctionMixins._load_replacement_values",
                original_exception=e
            ) from e



class TransformData(TxFunctionMixins):
    FUNCTIONS_WITH_TID = {
        'DeidentificationFirstName',
        'DeidentificationLastName',
        'DeidentificationFullName',
        'DeidentificationEmailAddress',
        'DeidentificationFacilityName'
    }

    def __init__(
        self,
        tx_list: t.List[t.List[t.Any]],
        header_mapping: t.Dict[str, int]
    ) -> None:
        self._tx_list: t.List[NT_TRANSFORMATIONS] = [
            NT_TRANSFORMATIONS(*tx) for tx in tx_list
        ]
        self._mapping: t.Dict[str, int] = header_mapping
        self._expr_engine = TransformationExpression(header_mapping)

    def apply_all_transformations(
        self,
        data_row: t.Union[t.List[t.Any], t.Tuple[t.Any]]
    ) -> t.List[t.Any]:
        data = list(data_row)
        transformed_data = []

        try:
            for tx in self._tx_list:
                try:
                    if tx.is_expr:
                        value = self._expr_engine.evaluate(tx.expression, data)
                        transformed_data.append(value)
                        continue

                    sname = tx.source_name
                    idx = self._mapping.get(sname)

                    if idx is None:
                        raise MorphusAirflowException(
                            message=f"Source column '{sname}' not found in header mapping",
                            error_source="TransformData.apply_all_transformations:column_lookup"
                        )

                    value = data[idx] if idx < len(data) else ""
                    transformed_data.append(
                        self.apply_transformation(
                            value,
                            tx.tx,
                            tx.teamID,
                            tx.tagID
                        )
                    )

                except MorphusAirflowException:
                    raise

                except Exception as e:
                    raise MorphusAirflowException(
                        message=f"Error applying transformation for source column '{tx.source_name}'",
                        error_source="TransformData.apply_all_transformations:per_tx",
                        original_exception=e
                    ) from e

            return transformed_data

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Unexpected error while applying transformations to data row",
                error_source="TransformData.apply_all_transformations",
                original_exception=e
            ) from e

    def apply_transformation(
        self,
        value: t.Any,
        txs: t.List[str],
        tid: t.Any,
        tagID: t.Any
    ) -> t.Any:
        ret = value

        try:
            for tx in txs:
                try:
                    method = getattr(self, tx)

                    if tx in self.FUNCTIONS_WITH_TID:
                        ret = method(ret, tid, tagID)
                    else:
                        ret = method(ret)

                except MorphusAirflowException:
                    raise

                except Exception as e:
                    raise MorphusAirflowException(
                        message=f"Error executing transformation '{tx}'",
                        error_source="TransformData.apply_transformation",
                        original_exception=e
                    ) from e

            return ret

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Unexpected error while applying transformation chain",
                error_source="TransformData.apply_transformation",
                original_exception=e
            ) from e
