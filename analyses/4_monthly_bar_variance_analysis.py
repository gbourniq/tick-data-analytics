from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from snowflake.snowpark.functions import col

from analyses.snowflake_utils import get_snowflake_connection


def plot_monthly_bar_variance_analysis():
    """
    Plot the monthly bar variance analysis for ES futures.
    """

    print("Plotting ES futures monthly bar variance analysis")

    # Connect to Snowflake
    session = get_snowflake_connection()

    # Create a table object for the monthly bar variance data
    monthly_bar_variance = session.table(
        "data_platform.fact.timeseries__es_equity_index_future__monthly_bar_variance"
    )

    # Convert Snowflake table to pandas DataFrame
    df = monthly_bar_variance.select(
        col("MONTH_START"), col("BAR_TYPE"), col("BAR_COUNT_VARIANCE")
    ).to_pandas()

    # Convert MONTH_START to datetime
    df["MONTH_START"] = pd.to_datetime(df["MONTH_START"])

    # Create a figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))

    # Plot 1: Monthly Bar Count Variance
    sns.lineplot(
        data=df,
        x="MONTH_START",
        y="BAR_COUNT_VARIANCE",
        hue="BAR_TYPE",
        marker="o",
        ax=ax1,
    )
    ax1.set_title("Monthly Bar Count Variance by Bar Type")
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Bar Count Variance")
    ax1.tick_params(axis="x", rotation=45)

    # Calculate variance of variances for each bar type
    variance_of_variances = (
        df.groupby("BAR_TYPE")["BAR_COUNT_VARIANCE"].var().reset_index()
    )

    # Add stability rank
    variance_of_variances["stability_rank"] = variance_of_variances[
        "BAR_COUNT_VARIANCE"
    ].rank()

    # Sort by stability rank
    variance_of_variances = variance_of_variances.sort_values("stability_rank")

    # Plot 2: Variance of Variances
    sns.barplot(
        data=variance_of_variances,
        x="BAR_TYPE",
        y="BAR_COUNT_VARIANCE",
        order=variance_of_variances["BAR_TYPE"],
        ax=ax2,
    )
    ax2.set_title("Variance of Monthly Bar Count Variances by Bar Type")
    ax2.set_xlabel("Bar Type")
    ax2.set_ylabel("Variance of Variances")

    # Add value labels on top of each bar
    for i, row in enumerate(variance_of_variances.itertuples()):
        ax2.text(
            i,
            row.BAR_COUNT_VARIANCE,
            f"{row.BAR_COUNT_VARIANCE:.6f}\nRank: {row.stability_rank:.0f}",
            ha="center",
            va="bottom",
        )

    plt.tight_layout()
    script_dir = Path(__file__).parent.resolve()
    plot_name = f"{Path(__file__).stem}.png"
    plot_path = script_dir / plot_name
    plt.savefig(plot_path, bbox_inches="tight")
    plt.close()
    print(f"Combined plot saved as {plot_path}\n\n")

    # Close the Snowflake session
    session.close()


if __name__ == "__main__":
    plot_monthly_bar_variance_analysis()
