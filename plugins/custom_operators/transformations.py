import json
import logging
import re
import sys
import typing as t
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from custom_operators.constants import NT_TRANSFORMATIONS

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
    def LowercaseToUppercase(self, val: str) -> str:
        if not isinstance(val, (str, type(None))):
            raise TypeError('Invalid type for val')
        return val.upper() if val else val

    def UppercaseToLowercase(self, val: str) -> str:
        if not isinstance(val, (str, type(None))):
            raise TypeError('Invalid type for val')
        return val.lower() if val else val

    def RemoveSpaces(self, val: str) -> str:
        if not isinstance(val, (str, type(None))):
            raise TypeError('Invalid type for val')
        return val.replace(' ', '') if val else val

    def StringToInt(self, val: str) -> int:
        if not isinstance(val, (str, type(None))):
            raise TypeError('Invalid type for val')
        if not val:
            return None
        try:
            return int(val)
        except ValueError:
            raise ValueError(f"Cannot convert {val} to int")

    def DeidentificationFirstName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, 'first_name')

    def DeidentificationLastName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, 'last_name')

    def DeidentificationFullName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, 'full_name')

    def DeidentificationEmailAddress(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, 'email_address')

    def DeidentificationFacilityName(self, val: str, tid: str, tagID: str) -> str:
        return self._deidentify_entity(val, tid, tagID, 'facility_name')

    def _deidentify_entity(self, val: str, tid: str, tagID: str, entity_type: str) -> str:
        if not isinstance(val, (str, type(None))):
            raise TypeError("Invalid type for val")
        if not isinstance(tid, str):
            raise TypeError("Invalid type for tid")
        if not isinstance(tagID, str):
            raise TypeError("Invalid type for tagID")
        if entity_type not in ENTITY_CONFIGS:
            raise ValueError(f"Unsupported entity type: {entity_type}")
        if not val:
            return val

        self._ensure_entity_pool(entity_type)
        path = self._tid_json_path(tid, tagID, entity_type)
        existing: t.Dict[str, str] = self._load_tid_mapping(path)

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

        replacement = memory_map.get(val)
        if replacement is None:
            replacement = self._get_next_replacement(entity_type)
        memory_map[val] = replacement
        setattr(self, memory_map_attr, memory_map)
        self._save_tid_mapping(path, {val: replacement})
        return replacement

    def _tid_json_path(self, tid: str, tagID: str, entity_type: str) -> str:
        if not isinstance(tid, str) or not tid.strip():
            raise TypeError("Invalid type for tid")
        if not isinstance(tagID, str) or not tagID.strip():
            raise TypeError("Invalid type for tagID")
        if entity_type not in ENTITY_CONFIGS:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        base_dir = getattr(self, "deid_json_dir", DEID_JSON_DIR)
        if isinstance(base_dir, str):
            base_dir = Path(base_dir)

        config = ENTITY_CONFIGS[entity_type]
        subdir = config['subdir']

        # Create directory structure: entity_type_maps/{tID}/{tagID}.json
        directory = base_dir / subdir / self._sanitize_filename(tid.strip())
        directory.mkdir(parents=True, exist_ok=True)

        safe_tagID = self._sanitize_filename(tagID.strip())
        if not safe_tagID:
            raise ValueError("tagID resulted in empty filename")

        return str(directory / f"{safe_tagID}.json")

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename by replacing invalid characters with underscores."""
        safe_filename = re.sub(r"[^A-Za-z0-9._-]+", "_", filename)
        return safe_filename

    def _load_tid_mapping(self, path: str) -> t.Optional[t.Dict[str, str]]:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _save_tid_mapping(self, path: str, mapping: t.Dict[str, str]) -> None:
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
        except OSError as exc:
            raise RuntimeError(f"Failed to save mapping to {path}") from exc

    def _get_next_replacement(self, entity_type: str) -> str:
        if entity_type not in ENTITY_CONFIGS:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        config = ENTITY_CONFIGS[entity_type]
        pool_attr = config['pool_attr']

        if not hasattr(self, pool_attr):
            raise RuntimeError(f"Replacement pool not initialized for {entity_type}")

        pool = getattr(self, pool_attr)
        if not pool:
            raise RuntimeError(f"Ran out of replacement {entity_type} values.")

        return pool.pop(0)

    def _ensure_entity_pool(self, entity_type: str) -> None:
        if entity_type not in ENTITY_CONFIGS:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        config = ENTITY_CONFIGS[entity_type]
        pool_attr = config['pool_attr']
        map_attr = config['map_attr']

        if not hasattr(self, pool_attr):
            file_path = getattr(self, f"{entity_type}_file_path", config['file_path'])
            replacement_pool = self._load_replacement_values(file_path)
            setattr(self, pool_attr, replacement_pool)

    @staticmethod
    def _load_replacement_values(path: str) -> t.List[str]:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except OSError as exc:
            raise FileNotFoundError(f'Failed to read replacements file: {path}') from exc

        if not lines:
            raise ValueError('Replacement file is empty; expected a header and data rows.')

        replacement_values = [line.strip() for line in lines[1:] if line.strip()]
        if not replacement_values:
            raise ValueError('No replacement values found after header.')
        return replacement_values


class TransformData(TxFunctionMixins):
    # Define which functions require tid parameter
    FUNCTIONS_WITH_TID = {
        'DeidentificationFirstName',
        'DeidentificationLastName',
        'DeidentificationFullName',
        'DeidentificationEmailAddress',
        'DeidentificationFacilityName'
    }

    def __init__(self,
                 tx_list: t.List[t.List[t.Any]],
                 header_mapping: t.Dict[str, int]) -> None:
        self._tx_list: t.List[NT_TRANSFORMATIONS] = [NT_TRANSFORMATIONS(*tx) for tx in tx_list]
        self._mapping: t.Dict[str, int] = header_mapping

    def apply_all_transformations(self, data_row: t.Union[t.List[t.Any], t.Tuple[t.Any]]) -> t.List[t.Any]:
        """
        Apply all transformations defined in tx_list to a single data row.

        Handles cases where some rows have fewer columns than the header by
        safely substituting empty strings instead of raising IndexError.
        """
        data = list(data_row)
        transformed_data = []

        for tx in self._tx_list:
            sname = tx.source_name
            tid = tx.teamID
            tagID = tx.tagID
            idx = self._mapping.get(sname, -1)

            if idx == -1:
                raise Exception(f"Source name {sname} not found in mapping")

            if idx < len(data):
                elem = data[idx]
            else:
                logging.warning(
                    f"[WARN] Missing column '{sname}' in data row — expected index {idx}, "
                    f"but row has only {len(data)} columns. Using empty string."
                )
                elem = ""  # pad missing columns with empty string

            s_tx = tx.tx
            transformed_data.append(self.apply_transformation(elem, s_tx, tid, tagID))

        return transformed_data

    def apply_transformation(self, value: t.Any, txs: t.List[str], tid: t.Any, tagID: t.Any) -> t.Any:
        ret = value
        for tx in txs:
            method = getattr(self, tx)
            if tx in self.FUNCTIONS_WITH_TID:
                ret = method(ret, tid, tagID)
            else:
                ret = method(ret)
        return ret