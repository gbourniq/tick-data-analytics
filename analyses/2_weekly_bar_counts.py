from pathlib import Path

import matplotlib.pyplot as plt
from snowflake.snowpark.functions import col

from analyses.snowflake_utils import get_snowflake_connection


def plot_weekly_bar_counts():
    """
    Plot the weekly bar counts for ES futures.
    """

    print("Plotting weekly bar counts for ES futures")

    # Connect to Snowflake
    session = get_snowflake_connection()

    # Create a table object for the weekly bar counts data
    weekly_bar_counts = session.table(
        "data_platform.fact.timeseries__es_equity_index_future__weekly_bar_counts"
    )

    # Select and order the data
    df = (
        weekly_bar_counts.select(
            col("WEEK_START"),
            col("TICK_BAR_COUNT"),
            col("VOLUME_BAR_COUNT"),
            col("DOLLAR_BAR_COUNT"),
        )
        .order_by("WEEK_START")
        .to_pandas()
    )

    # Rename all columns to lowercase
    df.columns = df.columns.str.lower()

    # Create the plot
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(15, 30))

    # Function to plot data for a specific time range
    def plot_data(ax, start_date, end_date, title):
        mask = (df["week_start"] >= start_date) & (df["week_start"] < end_date)
        ax.plot(
            df.loc[mask, "week_start"], df.loc[mask, "tick_bar_count"], label="Tick Bars"
        )
        ax.plot(
            df.loc[mask, "week_start"],
            df.loc[mask, "volume_bar_count"],
            label="Volume Bars",
        )
        ax.plot(
            df.loc[mask, "week_start"],
            df.loc[mask, "dollar_bar_count"],
            label="Dollar Bars",
        )
        ax.set_title(title)
        ax.set_xlabel("Date")
        ax.set_ylabel("Bar Count")
        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.7)
        ax.tick_params(axis="x", rotation=45)

    # Plot data for each time period
    plot_data(ax1, df["week_start"].min(), "2008-01-01", "Weekly Bar Counts Until 2008")
    plot_data(ax2, "2008-01-01", "2012-01-01", "Weekly Bar Counts 2008-2012")
    plot_data(
        ax3, "2012-01-01", df["week_start"].max(), "Weekly Bar Counts 2012 Onwards"
    )

    # Calculate the coefficient of variation for each bar type
    cv_data = {}
    for bar_type in ["tick_bar_count", "volume_bar_count", "dollar_bar_count"]:
        cv = df[bar_type].std() / df[bar_type].mean()
        cv_data[bar_type] = cv

    # Create a bar chart for coefficients of variation
    bar_types = list(cv_data.keys())
    cv_values = list(cv_data.values())
    ax4.bar(bar_types, cv_values)
    ax4.set_title("Coefficient of Variation by Bar Type")
    ax4.set_xlabel("Bar Type")
    ax4.set_ylabel("Coefficient of Variation")
    ax4.tick_params(axis="x", rotation=45)
    for i, v in enumerate(cv_values):
        ax4.text(i, v, f"{v:.4f}", ha="center", va="bottom")

    # Save the plot
    script_dir = Path(__file__).parent.resolve()
    plot_name = f"{Path(__file__).stem}.png"
    plot_path = script_dir / plot_name
    plt.savefig(plot_path, bbox_inches="tight")
    print(f"Plot saved as {plot_path}\n\n")

    session.close()


if __name__ == "__main__":
    plot_weekly_bar_counts()
