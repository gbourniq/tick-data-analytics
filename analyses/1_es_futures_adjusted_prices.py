from pathlib import Path

import matplotlib.pyplot as plt
from snowflake.snowpark.functions import col, date_trunc

from analyses.snowflake_utils import get_snowflake_connection


def plot_es_futures_adjusted():
    """
    Plot the adjusted prices for ES futures.
    """

    print("Plotting ES futures adjusted prices")

    # Connect to Snowflake
    session = get_snowflake_connection()

    # Create a table object for the ES futures data after adjustment
    es_futures = session.table(
        "data_platform.transform.int__es_equity_index_future__continuous"
    )

    print(f"Number of rows in the ES futures table: {es_futures.count():,}")

    # Sample the data and aggregate by day to reduce the number of points
    sampled_data = (
        es_futures.select(
            date_trunc("DAY", col("trading_datetime")).alias("date"),
            col("contract_symbol"),
            col("price"),
        )
        .groupBy("date", "contract_symbol")
        .agg({"price": "mean"})
        .withColumnRenamed("AVG(PRICE)", "avg_price")
        .sample(n=10000)
    )

    # Convert to pandas DataFrame
    df = sampled_data.to_pandas()

    # Create the plot
    plt.figure(figsize=(12, 8))  # Increased figure height to accommodate legend
    for symbol in df["CONTRACT_SYMBOL"].unique():
        symbol_data = df[df["CONTRACT_SYMBOL"] == symbol]
        plt.plot(symbol_data["DATE"], symbol_data["AVG_PRICE"], label=symbol)

    plt.title("Adjusted Prices for ES Equity Index Futures")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.xticks(rotation=45)

    # Move legend below the plot
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc="upper center", ncol=3)

    # Adjust layout to prevent cutting off the legend
    plt.tight_layout()

    # Save the plot
    script_dir = Path(__file__).parent.resolve()
    plot_name = f"{Path(__file__).stem}.png"
    plot_path = script_dir / plot_name
    plt.savefig(plot_path, bbox_inches="tight")
    print(f"Plot saved as {plot_path}\n\n")

    session.close()


if __name__ == "__main__":
    plot_es_futures_adjusted()
