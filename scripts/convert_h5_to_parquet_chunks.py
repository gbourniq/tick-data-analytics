"""
This script reads HDF5 files, processes each dataset within, and outputs multiple smaller
Parquet files, making the data more suitable for efficient ingestion into Snowflake.
"""

from pathlib import Path

import h5py
import numpy
import pyarrow as pa
import pyarrow.parquet as pq


def read_h5_datasets(h5file):
    datasets = {}

    def visitor_func(name, node):
        if isinstance(node, h5py.Dataset):
            datasets[name] = node

    h5file.visititems(visitor_func)
    return datasets


def numpy_dtype_to_pa_type(numpy_dtype):
    """Map NumPy dtype to PyArrow type considering bit widths."""
    if numpy_dtype.kind == "i":
        # Signed integer
        if numpy_dtype.itemsize == 1:
            return pa.int8()
        elif numpy_dtype.itemsize == 2:
            return pa.int16()
        elif numpy_dtype.itemsize == 4:
            return pa.int32()
        elif numpy_dtype.itemsize == 8:
            return pa.int64()
    elif numpy_dtype.kind == "u":
        # Unsigned integer
        if numpy_dtype.itemsize == 1:
            return pa.uint8()
        elif numpy_dtype.itemsize == 2:
            return pa.uint16()
        elif numpy_dtype.itemsize == 4:
            return pa.uint32()
        elif numpy_dtype.itemsize == 8:
            return pa.uint64()
    elif numpy_dtype.kind == "f":
        # Floating point
        if numpy_dtype.itemsize == 4:
            return pa.float32()
        elif numpy_dtype.itemsize == 8:
            return pa.float64()
    elif numpy_dtype.kind == "S":
        # Fixed-length byte strings
        return pa.string()
    else:
        raise ValueError(
            f"Unsupported numpy dtype kind '{numpy_dtype.kind}' with itemsize"
            f" {numpy_dtype.itemsize}"
        )


def estimate_chunk_size(dataset, target_file_size_mb):
    """Estimate chunk size based on dataset dtype and target file size."""
    # Calculate the size of one row in bytes
    row_size_bytes = dataset.dtype.itemsize
    # Estimate number of rows to reach the target file size
    target_file_size_bytes = target_file_size_mb * 1024 * 1024
    estimated_rows = target_file_size_bytes // row_size_bytes
    return max(1, int(estimated_rows))


def convert_h5_to_parquet(input_file: Path, target_file_size_mb=200):
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    with h5py.File(input_file, "r") as h5file:
        datasets = read_h5_datasets(h5file)

        if not datasets:
            raise ValueError(f"No datasets found in {input_file}")

        print(f"Found {len(datasets)} datasets in {input_file}")
        for name, dataset in datasets.items():
            print(f"Dataset: {name}, Shape: {dataset.shape}, Type: {dataset.dtype}")

        # Processing datasets (single or multiple)
        output_dir = input_file.with_suffix(
            ""
        )  # Remove file extension for output directory
        output_dir.mkdir(exist_ok=True)
        for name, dataset in datasets.items():
            try:
                total_rows = dataset.shape[0]
                # Estimate chunk size to aim for target file size
                chunk_size = estimate_chunk_size(dataset, target_file_size_mb)
                print(
                    f"Estimated chunk size for dataset '{name}': {chunk_size} rows per"
                    " file"
                )

                # Get field names and data types
                if dataset.dtype.names:
                    # Structured array with named fields
                    field_names = dataset.dtype.names
                else:
                    # Regular array without named fields
                    field_names = [f"col{i}" for i in range(dataset.shape[1])]
                len(field_names)

                # Prepare output file base path
                sanitized_name = name.replace("/", "_").replace(" ", "_")
                dataset_output_base = output_dir / f"{sanitized_name}_part"

                # Prepare schema with accurate type mapping
                schema_fields = []
                for field_name in field_names:
                    numpy_dtype = dataset.dtype[field_name]
                    pa_type = numpy_dtype_to_pa_type(numpy_dtype)
                    schema_fields.append(pa.field(field_name, pa_type))
                pa.schema(schema_fields)

                part_num = 0
                for start in range(0, total_rows, chunk_size):
                    end = min(start + chunk_size, total_rows)
                    data_chunk = dataset[start:end]

                    arrays = []
                    for field_name in field_names:
                        numpy_array = data_chunk[field_name]
                        numpy_dtype = dataset.dtype[field_name]
                        if numpy_dtype.kind == "S":
                            # Convert byte strings to Unicode strings
                            numpy_array = numpy.char.decode(
                                numpy_array, "utf-8", errors="replace"
                            )
                            pa_array = pa.array(numpy_array, type=pa.string())
                        elif numpy_dtype.kind in ("i", "u", "f"):
                            pa_array = pa.array(
                                numpy_array, type=numpy_dtype_to_pa_type(numpy_dtype)
                            )
                        else:
                            raise ValueError(
                                f"Unsupported numpy dtype kind '{numpy_dtype.kind}' for"
                                f" field '{field_name}'"
                            )
                        arrays.append(pa_array)
                    table = pa.Table.from_arrays(arrays, names=field_names)

                    # Write each chunk to a separate Parquet file
                    output_file = dataset_output_base.with_name(
                        f"{dataset_output_base.stem}_{part_num:04d}.parquet"
                    )
                    pq.write_table(table, output_file, compression="snappy")
                    print(
                        f"Wrote rows {start} to {end} for dataset {name} to file"
                        f" {output_file.name}"
                    )
                    part_num += 1
                print(f"Converted dataset {name} into {part_num} files in {output_dir}")
            except Exception as e:
                print(f"Error processing dataset {name}: {str(e)}")

        print(f"Conversion completed. Output files are located in {output_dir}")


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    input_file = current_dir.parent / "data" / "ES.h5"
    convert_h5_to_parquet(input_file, target_file_size_mb=1024)
