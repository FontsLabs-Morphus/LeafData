def normalize_header(header: list, data_row: list) -> list:
    """
    Handles:
    - Fully empty header
    - Partially empty header
    - Header shorter than data

    Generates: Col1, Col2, Col3 ... for blank positions.
    """
    final_header = []
    total_cols = max(len(header), len(data_row))

    for i in range(total_cols):
        if i < len(header) and header[i].strip() != "":
            final_header.append(header[i].strip())
        else:
            final_header.append(f"Col{i+1}")

    return final_header