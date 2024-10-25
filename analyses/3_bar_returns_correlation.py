from pathlib import Path

import matplotlib.pyplot as plt
from snowflake.snowpark.functions import col

from analyses.snowflake_utils import get_snowflake_connection


def plot_es_futures_bar_returns_correlation():
    """
    Plot the serial correlation of bar returns for ES futures.
    """

    print("Plotting ES futures bar returns correlation")

    # Connect to Snowflake
    session = get_snowflake_connection()

    # Create a table object for the weekly bar counts data
    bar_returns_correlation = session.table(
        "data_platform.fact.timeseries__es_equity_index_future__bar_returns_correlation"
    )

    # Convert Snowflake table to pandas DataFrame
    df = bar_returns_correlation.select(
        col("BAR_TYPE"), col("SERIAL_CORRELATION")
    ).to_pandas()

    # Create a bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(df["BAR_TYPE"], df["SERIAL_CORRELATION"])
    plt.title("Serial Correlation by Bar Type")
    plt.xlabel("Bar Type")
    plt.ylabel("Serial Correlation")

    # Add value labels on top of each bar
    for i, v in enumerate(df["SERIAL_CORRELATION"]):
        plt.text(i, v, f"{v:.6f}", ha="center", va="bottom")

    # Adjust y-axis to highlight differences
    plt.ylim(
        min(df["SERIAL_CORRELATION"]) - 0.001, max(df["SERIAL_CORRELATION"]) + 0.001
    )

    # Add a horizontal line at y=0 for reference
    plt.axhline(y=0, color="r", linestyle="--", alpha=0.5)

    # Save the plot
    script_dir = Path(__file__).parent.resolve()
    plot_name = f"{Path(__file__).stem}.png"
    plot_path = script_dir / plot_name
    plt.savefig(plot_path, bbox_inches="tight")
    print(f"Plot saved as {plot_path}\n\n")
    plt.close()

    # Close the Snowflake session
    session.close()


if __name__ == "__main__":
    plot_es_futures_bar_returns_correlation()
