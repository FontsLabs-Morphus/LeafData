import io
import os
from typing import Dict, Tuple, Union, List

import avro.io
import avro.schema
from avro.errors import SchemaParseException
from avro_schema.exceptions import (NoneErrorException, AvroException, InvalidSchemaException,
                                        SchemaFieldMissingException)


def load_avro_schema_from_file(avro_schema_path: str) -> avro.schema.Schema:
    if not avro_schema_path:
        raise NoneErrorException("avroSchemaPath Parameter not constructed. Cannot access a 'None' object.")

    if not os.path.isfile(avro_schema_path):
        raise AvroException(f"Schema file not found at path: {avro_schema_path}")

    try:
        with open(avro_schema_path, 'r') as schema_file:
            schema_str = schema_file.read()
        return avro.schema.parse(schema_str)
    except SchemaParseException as e:
        raise AvroException(
            f"Failed to parse schema. Check that the Avro schema is properly structured: {str(e)}")
    except Exception as e:
        raise AvroException(f"An error occurred while reading the schema file: {str(e)}")


def ensure_defaults_in_avro_schema(json_data: Dict, schema: avro.schema.Schema) -> Dict:
    if not json_data:
        raise NoneErrorException("jsonData Parameter not constructed. Cannot access a 'None' object.")

    if not isinstance(schema, avro.schema.Schema):
        raise InvalidSchemaException("The provided schema is not a valid Avro schema object.")

    try:
        for field in schema.fields:
            if field.name not in json_data and 'default' in field.props:
                json_data[field.name] = field.props['default']
        return json_data
    except KeyError as e:
        raise SchemaFieldMissingException(f"Schema field missing in JSON data: {str(e)}")
    except Exception as e:
        raise AvroException(f"An error occurred while ensuring defaults: {str(e)}")


def get_schema_field_names(schema: avro.schema.Schema) -> List[str]:
    return [field.name for field in schema.fields]


def get_missing_fields(json_data: Dict, schema: avro.schema.Schema) -> List[str]:
    schema_fields = get_schema_field_names(schema)
    return [field for field in schema_fields if field not in json_data]


def get_extra_fields(json_data: Dict, schema: avro.schema.Schema) -> List[str]:
    schemaFields = get_schema_field_names(schema)
    return [field for field in json_data if field not in schemaFields]


def validate_json_against_avro(json_data: Dict, avro_schema_path: str) -> Tuple[bool, Union[None, str]]:
    if not json_data:
        raise NoneErrorException("jsonData Parameter not constructed. Cannot access a 'None' object.")
    if avro_schema_path is None or not avro_schema_path:
        raise NoneErrorException("avroSchemaPath Parameter not constructed. Cannot access a 'None' object.")
    if not isinstance(json_data, dict):
        raise AvroException("Provided JSON data is not a valid dictionary.")

    try:
        avro_schema = load_avro_schema_from_file(avro_schema_path)
        json_data = ensure_defaults_in_avro_schema(json_data, avro_schema)
        missing_fields = get_missing_fields(json_data, avro_schema)
        if missing_fields:
            return False, f"Validation failed. Missing columns from the data: {missing_fields}"
        extra_fields = get_extra_fields(json_data, avro_schema)
        if extra_fields:
            return False, f"Validation failed. Extra columns in the data: {extra_fields}. Expected fields: {get_schema_field_names(avro_schema)}"

        buffer = io.BytesIO()
        writer = avro.io.DatumWriter(avro_schema)
        encoder = avro.io.BinaryEncoder(buffer)
        writer.write(json_data, encoder)

        return True, None
    except FileNotFoundError:
        return False, f"Schema file not found at path: {avro_schema_path}"
    except AvroException as e:
        return False, str(e)
    except Exception as e:
        return False, f"An unexpected error occurred: {str(e)}"
