from dataclasses import dataclass
import pandas as pd


@dataclass
class Fidelity_BR_Data:
    sum: pd.DataFrame
    bt: pd.DataFrame
    chin: pd.DataFrame
    chou: pd.DataFrame

@dataclass
class PimcoData:
    sum: pd.DataFrame
    bt: pd.DataFrame
