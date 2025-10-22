from fmctools import fmccfg, fmccog, fmccsv, fmcmsx, fmcpol, fmcsql
import polars as pl


def add_race_cols(df):
    result = df.with_columns(
        race_native=pl.when(pl.col("Race").str.contains("American"))
        .then(1)
        .otherwise(0),
        race_asian=pl.when(pl.col("Race").str.contains("Asian")).then(1).otherwise(0),
        race_black=pl.when(pl.col("Race").str.contains("Black")).then(1).otherwise(0),
        race_pacific=pl.when(pl.col("Race").str.contains("Pacific"))
        .then(1)
        .otherwise(0),
        race_white=pl.when(pl.col("Race").str.contains("White")).then(1).otherwise(0),
        race_other=pl.when(pl.col("Race").str.contains("Other")).then(1).otherwise(0),
        race_decline=pl.when(pl.col("Race").str.contains("Prefer"))
        .then(1)
        .otherwise(0),
    )
    return result


def add_eth_cols(df):
    result = df
    result = result.with_columns(
        eth_hispanic=pl.when(pl.col("Hispanic").str.contains("Yes "))
        .then(1)
        .when(pl.col("Hispanic").str.contains("Prefer"))
        .then(2)
        .otherwise(0),
        eth_arab=pl.when(pl.col("Arab").str.contains("Yes "))
        .then(1)
        .when(pl.col("Arab").str.contains("Prefer"))
        .then(2)
        .otherwise(0),
    )
    return result


def add_gender_col(df):
    result = df
    result = result.with_columns(
        gender=pl.when(pl.col("Gender").str.contains("Male"))
        .then(pl.lit("Male"))
        .when(pl.col("Gender").str.contains("Female"))
        .then(pl.lit("Female"))
        .otherwise(None)
    )
    return result


def add_consent_cols(df):
    result = df
    result = result.with_columns(
        phone_consent=pl.when(pl.col("PhoneConsent").str.contains("Yes"))
        .then(1)
        .otherwise(0),
        email_consent=pl.when(pl.col("EmailConsent").str.contains("Yes"))
        .then(1)
        .otherwise(0),
    )
    return result


def fix_dates(df):
    result = df
    result = result.with_columns(
        date_birth=pl.date("Year", "Month", "Day"),
        submission_date=pl.col("Entry_DateSubmitted").str.slice(0, 10),
    )
    result = result.cast({"submission_date": pl.Date})
    return result


def format_conference(df):
    result = df
    result = result.with_columns(
        conference_name=pl.col("Conference").str.split(" - ").list.first(),
        conference_id=pl.col("Conference").str.split(" - ").list.last(),
    )
    return result


def format_surveys(df, col_rename, col_select):
    result = df
    result = add_race_cols(result)
    result = add_eth_cols(result)
    result = add_gender_col(result)
    result = add_consent_cols(result)
    result = fix_dates(result)
    result = format_conference(result)
    result = result.rename(col_rename)
    result = result.select(col_select)
    return result


def main():
    cfg = fmccfg.read_toml("minister_surveys_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    api_key = cfg["cognito"]["api_key"]
    form_id = cfg["cogministersurveys"]["form_id"]
    surveys = fmccog.get_form_ret_df(api_key, form_id)
    surveys = format_surveys(
        surveys,
        cfg["cogministersurveys"]["col_rename"],
        cfg["cogministersurveys"]["col_select"],
    )
    print(surveys)
    fmccsv.writecsv_from_frame(surveys, "cog_minister_surveys.csv")
    print("local csv written...")
    surveys.write_database(
        table_name="s_cog_minister_surveys",
        connection=FMCSQL.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    print("s_cog_minister_surveys updated...")
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "exec u_ministers_surveys")
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "DROP TABLE s_cog_minister_surveys")
    fmcmsx.refresh_excel(cfg["msx_reports"]["mailing_list_path"])


if __name__ == "__main__":
    main()
