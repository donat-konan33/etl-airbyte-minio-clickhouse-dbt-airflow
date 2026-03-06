# here are some functions used transform data into local storage, we can adjust them for a datalake utilization

import pathlib
from typing import List, Optional
import pandas as pd
import unicodedata, re


class GetExistingPaths:
    """
    This class contains functions that returns a list of paths to existing files in a folder
    """
    def __init__(self, folder_path: str, file_format: str):
        """
        This function returns a list of paths to existing files in folder that path is given and ending with pattern added

        Example:
        folder_path : "data/old_data"
        format : "france_data*.csv"

        """
        self.folder_path = folder_path
        self.file_format = file_format

    @classmethod
    def call_get_existing_subgroup(cls, folder_path: str, file_format: str) -> List[pathlib.Path]:
        """
        create an unitialized instance of the class and call the get_existing_paths method
        """
        return cls(folder_path, file_format)

    def get_existing_paths(self) -> List[pathlib.Path]:
        """
        return list of existing files mathing with given format"""

        files_path = list(pathlib.Path(self.folder_path).glob(self.file_format))
        return files_path

    def __repr__(self):
        """
        display existing file in folder
        """
        return f"GetExistingPaths(folder_path='{self.folder_path}', file_format='{self.file_format}')"



class TransformData:
    """
    set of method that transform data
    """

    @staticmethod
    def transform_regdep_france(staging_path: str, data_path: Optional[str] = None) -> None:
        """
        transform only regdep_france
        default data path is data/opendatasoft_2024/regdep_france.parquet otherwise from a bucket, always from
        regdep_france.parquet
        """
        data_path = data_path or "data/opendatasoft_2024/regdep_france.parquet"
        df = pd.read_parquet(data_path)
        df = df[['geo_point_2d', 'geo_shape', 'reg_code', 'reg_name',
                    'dep_current_code', 'dep_name_upper', 'dep_status']]
        df.to_parquet(staging_path, index=False)

    @staticmethod
    def normalize(data: pd.Series):
        """
        Parameters:
        -----------
            data pandas column to noramlize

        Returns:
        --------
            Normalize data
        """

        return (
            data.astype(str)
            .str.normalize("NFD")                 # split letters and accents
            .str.encode("ascii", "ignore")        # remove accents
            .str.decode("utf-8")
            .str.replace("-", " ", regex=False)   # replace dash
            .str.replace("'", " ", regex=False)   # remove apostrophes
            .str.replace(r"\s+", " ", regex=True) # remove double spaces
            .str.strip()                          # remove leading/trailing spaces
            .str.upper()
        )
