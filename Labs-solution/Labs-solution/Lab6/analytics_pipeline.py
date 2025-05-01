import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

from prefect import task, flow

DEFAULT_CSV = Path("analytics_data.csv")
SUMMARY_PATH = Path("analytics_summary.csv")
HISTOGRAM_PATH = Path("sales_histogram.png")

@task(retries=2, retry_delay_seconds=30)
def fetch_data(path: Path) -> pd.DataFrame:
    print(f"Reading data from {path}")
    df = pd.read_csv(path)
    print(f"Loaded {df.shape[0]} rows × {df.shape[1]} cols")
    return df

@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    missing = df.isnull().sum()
    print("Missing values by column:\n", missing)
    clean_df = df.dropna().reset_index(drop=True)
    print(f"Dropped {df.shape[0] - clean_df.shape[0]} rows; remaining {clean_df.shape[0]}")
    return clean_df

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    if "sales" in df.columns:
        df = df.copy()
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
        print("Added sales_normalized column")
    else:
        print("No sales column found — skipping normalization")
    return df

@task
def generate_summary(df: pd.DataFrame, output_path: Path) -> Path:
    df.describe().to_csv(output_path)
    print(f"Saved summary stats → {output_path}")
    return output_path

@task
def plot_sales_histogram(df: pd.DataFrame, output_path: Path) -> Path:
    if "sales" not in df.columns:
        print("No sales column — skipping histogram")
        return output_path

    plt.hist(df["sales"], bins=20)
    plt.title("Sales Distribution")
    plt.xlabel("Sales")
    plt.ylabel("Frequency")
    plt.savefig(output_path)
    plt.close()
    print(f"Saved histogram → {output_path}")
    return output_path

@flow(name="Analytics Pipeline")
def main(
    csv_path: Path = DEFAULT_CSV,
    summary_path: Path = SUMMARY_PATH,
    histogram_path: Path = HISTOGRAM_PATH,
):
    raw_df = fetch_data(csv_path)
    clean_df = validate_data(raw_df)
    transformed_df = transform_data(clean_df)
    generate_summary(transformed_df, summary_path)
    plot_sales_histogram(transformed_df, histogram_path)

if __name__ == "__main__":
    main()
