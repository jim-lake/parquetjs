import math

def _vlq_encode(value):
    """Encodes an integer into VLQ (Variable-Length Quantity) bytes."""
    encoded_bytes = bytearray()
    while True:
        byte = value & 0x7F  # Get lower 7 bits
        value >>= 7
        if value == 0:
            encoded_bytes.append(byte)
            break
        else:
            byte |= 0x80  # Set MSB to indicate more bytes are coming
            encoded_bytes.append(byte)
    return encoded_bytes

def _encode_repeated_run(value, count, bit_width):
    """
    Encodes a run of repeated values.
    Format:
    - VLQ-encoded header: (count << 1)
    - Value: bit-packed to the smallest number of bytes required for bit_width
    """
    if count == 0:
        return bytearray()

    header = _vlq_encode(count << 1)

    # Calculate bytes needed for the value and pack it in little-endian format
    num_bytes = (bit_width + 7) // 8

    # Handle the case of bit_width 0, where no value is written
    if num_bytes == 0:
        return header

    packed_value = value.to_bytes(num_bytes, byteorder='little', signed=False)

    return header + packed_value

def _encode_bitpacked_run(literals, bit_width):
    """
    Encodes a run of literal (non-repeating) values.
    Format:
    - VLQ-encoded header: (num_groups << 1) | 1
    - Data: All values bit-packed into a contiguous byte stream
    """
    if not literals:
        return bytearray()

    num_literals = len(literals)
    # A "group" in the Parquet spec consists of 8 values. The header indicates the number of groups.
    num_groups = math.ceil(num_literals / 8)
    header = _vlq_encode((num_groups << 1) | 1)

    # The actual bit-packing implementation
    packed_data = bytearray()
    buffer = 0
    bits_in_buffer = 0

    for value in literals:
        # Add the new value's bits to the buffer, starting from the current LSB position
        buffer |= (value << bits_in_buffer)
        bits_in_buffer += bit_width

        # While the buffer has enough bits to form a full byte, flush it
        while bits_in_buffer >= 8:
            packed_data.append(buffer & 0xFF)
            buffer >>= 8
            bits_in_buffer -= 8

    # If any bits remain in the buffer, flush them to a final byte
    if bits_in_buffer > 0:
        packed_data.append(buffer & 0xFF)

    # The total number of bytes should be based on the number of groups, not just the packed literals.
    # This pads the output to the required length for the declared number of groups.
    total_bytes_for_groups = math.ceil(num_groups * 8 * bit_width / 8)
    padding_bytes = total_bytes_for_groups - len(packed_data)
    if padding_bytes > 0:
        packed_data.extend(b'\x00' * padding_bytes)

    return header + packed_data

def rle_encode_parquet(values, bit_width):
    """
    Performs RLE/Bit-Packing encoding on a list of integer values, compatible
    with the Parquet column encoding specification.

    Args:
        values (list[int]): A list of non-negative integers to encode.
        bit_width (int): The number of bits required to store the largest value.

    Returns:
        bytearray: The RLE/Bit-Packed encoded data.
    """
    if not values:
        return bytearray()

    output = bytearray()
    i = 0
    n = len(values)
    # Per the Apache Parquet spec, a run of at least 8 identical values is RLE-encoded.
    RLE_THRESHOLD = 8

    while i < n:
        # 1. Find the length of the run of the current value
        j = i
        while j < n and values[j] == values[i]:
            j += 1
        run_length = j - i

        if run_length >= RLE_THRESHOLD:
            # 2. If the run is long enough, encode it as a repeated run
            output.extend(_encode_repeated_run(values[i], run_length, bit_width))
            i += run_length
        else:
            # 3. Otherwise, find the end of a block of literal values
            literals_end = i
            while literals_end < n:
                # Look ahead to see if a long run is about to start
                k = literals_end
                while k < n and values[k] == values[literals_end]:
                    k += 1

                if (k - literals_end) >= RLE_THRESHOLD:
                    # A long run starts here, so the literal block must end before it
                    break
                else:
                    # This short run is part of our literal block
                    literals_end = k

            # 4. Encode the collected literals as a bit-packed run
            literals = values[i:literals_end]
            output.extend(_encode_bitpacked_run(literals, bit_width))
            i = literals_end

    return output

# --- Main execution block to demonstrate the encoder ---
if __name__ == "__main__":
    import sys
    import ast

    if len(sys.argv) != 3:
        print("Usage: python parquet_rle_encoder.py <bit_width> \"<list_of_integers>\"")
        print("Example: python parquet_rle_encoder.py 21 \"[1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567]\"")
        sys.exit(1)

    try:
        bit_width_arg = int(sys.argv[1])
        if bit_width_arg < 0:
            raise ValueError("Bit width cannot be negative.")

        values_str_arg = sys.argv[2]

        # Safely evaluate the string representation of the list
        input_values = ast.literal_eval(values_str_arg)

        if not isinstance(input_values, list) or not all(isinstance(x, int) for x in input_values):
             raise ValueError("Second argument must be a string representation of a list of integers.")

    except (ValueError, SyntaxError) as e:
        print(f"Error parsing arguments: {e}")
        print("Please ensure the second argument is a valid list of integers in string format, e.g., \"[1, 2, 3]\"")
        sys.exit(1)

    print(f"Input: {input_values}")
    print(f"Bit Width: {bit_width_arg}")

    encoded_output = rle_encode_parquet(input_values, bit_width_arg)
    hex_output = ', '.join(f'0x{byte:02x}' for byte in encoded_output)

    print(f"Encoded Output (bytes): {encoded_output}")
    print(f"Encoded Output (hex):   [{hex_output}]")

