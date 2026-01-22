import pandas as pd
from pydantic import BaseModel


class Records(BaseModel):
    data: list[dict]


class DataProcessing:

    @staticmethod
    def convert_to_df(file):
        try:
            df = pd.DataFrame(file)
            return df
        except Exception as e:
            raise Exception(f"codent convert file to df, Error:{e}")

    @staticmethod
    def get_risk_level_colume(df):
        try:

            print(df)
            df["risk_level"] = pd.cut(
                df["range_km"],
                [0, 20, 100, 300, float("inf")],
                labels=["low", "medium", "high", "extreme"],
                include_lowest=True,
            ).astype(str)

            return df

        except Exception as e:
            raise Exception(f"codent add aditinal colum 'risk_level', Error:{e}")

    @staticmethod
    def replace_noen_values_whit_Unknown(df):
        try:
            df["manufacturer"] = df["manufacturer"].replace(["NULL"], " Unknown ")
            df["manufacturer"] = df["manufacturer"].replace(["NaN"], " Unknown ")
            return df
        except Exception as e:
            raise Exception(f"codent relace '[NULL,NaN]' whit 'Unknown', Error:{e}")

    @staticmethod
    def convert_df_to_json(df):
        try:
            list_df = df.to_dict("records")
            data = Records(data=list_df)
            return data.model_dump(mode="json")
        except Exception as e:
            raise Exception(f"codent convert df to json, Error:{e}")

    @staticmethod
    def data_processing_ful_tesk(df):
        try:
            ndf = DataProcessing.get_risk_level_colume(df)
            fdf = DataProcessing.replace_noen_values_whit_Unknown(ndf)
            json_data = DataProcessing.convert_df_to_json(fdf)
            return json_data
        except Exception as e:
            raise Exception(f"codent use complit tesk, Error:{e}")
