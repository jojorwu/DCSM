from .generated_grpc_code import kem_pb2
from google.protobuf.json_format import MessageToDict, ParseDict
import base64 # For correct processing of the content field, if it is binary

def kem_dict_to_proto(kem_data: dict) -> kem_pb2.KEM:
    """
    Converts a Python dictionary to a KEM protobuf message.
    Clears Timestamp fields if they are not strings, as ParseDict expects them as RFC 3339 strings.
    Encodes string 'content' to bytes.
    """
    kem_data_copy = kem_data.copy()

    # Timestamps from dict (e.g., from user input or other sources) might not be
    # in the string format expected by ParseDict for google.protobuf.Timestamp.
    # If they are datetime objects, they should be converted to string or Timestamp proto first.
    # For simplicity, if not a string, remove them, assuming the server will set them if needed.
    if 'created_at' in kem_data_copy and not isinstance(kem_data_copy['created_at'], str):
        del kem_data_copy['created_at']
    if 'updated_at' in kem_data_copy and not isinstance(kem_data_copy['updated_at'], str):
        del kem_data_copy['updated_at']

    # The 'content' field in KEM proto is bytes. If it's a string in the dict, encode to utf-8.
    # If it's already bytes, ParseDict should handle it.
    if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
        kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')

    # Ensure embeddings are a list of floats, protobuf can be strict.
    if 'embeddings' in kem_data_copy and isinstance(kem_data_copy['embeddings'], list):
        try:
            kem_data_copy['embeddings'] = [float(e) for e in kem_data_copy['embeddings']]
        except (ValueError, TypeError):
            # Handle cases where conversion to float might fail, e.g. by removing or logging.
            # For now, if conversion fails, remove the embeddings field.
            del kem_data_copy['embeddings']

    # Ensure metadata is map<string, string>.
    if 'metadata' in kem_data_copy and isinstance(kem_data_copy['metadata'], dict):
        kem_data_copy['metadata'] = {str(k): str(v) for k, v in kem_data_copy['metadata'].items()}

    return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

def kem_proto_to_dict(kem_proto: kem_pb2.KEM) -> dict:
    """
    Converts a KEM protobuf message to a Python dictionary.
    Attempts to decode the 'content' field from bytes to a UTF-8 string.
    MessageToDict converts bytes to base64 strings, so this function decodes from base64, then from utf-8.
    """
    # including_default_value_fields=True ensures that fields with default values (e.g., empty strings, 0 for numbers) are included in the dict.
    kem_dict = MessageToDict(kem_proto, preserving_proto_field_name=True, including_default_value_fields=True)

    # The 'content' field (bytes in proto) is converted by MessageToDict to a base64-encoded string.
    # Decode it back to bytes, then attempt to decode to a utf-8 string.
    if 'content' in kem_dict and isinstance(kem_dict['content'], str):
        try:
            decoded_bytes = base64.b64decode(kem_dict['content'])
            try:
                kem_dict['content'] = decoded_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # If not UTF-8, it might be binary data.
                # For consistency, if it was text, it should have been UTF-8.
                # If it's binary, what should be returned?
                # Option 1: Return the raw bytes: kem_dict['content'] = decoded_bytes (changes field type in dict)
                # Option 2: Return the base64 string as is (current behavior of MessageToDict)
                # Option 3: Add a separate field like 'content_bytes'.
                # For now, if UTF-8 decoding fails, leave it as a base64 string, assuming the client can handle it
                # or it signifies non-textual binary data.
                 pass # Content remains a base64 string if not valid utf-8
        except Exception:
            # Error decoding base64 (unlikely if from MessageToDict), leave content as is.
            pass

    # Embeddings will already be a list of floats or numbers.
    # Metadata will already be a dictionary.
    # Timestamps (created_at, updated_at) will be RFC 3339 strings.
    return kem_dict

# Similar utilities can be added for other proto messages if needed.
