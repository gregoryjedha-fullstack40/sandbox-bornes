import pytest
import pandas as pd
from etl import collect_data

import pytest
import pandas as pd


@pytest.fixture
def mock_bornes():
    data = [
        {
            "id_pdc_itinerance": "FRV75EPX12205",
            "id_station_itinerance": "FRV75PPX1220",
            "nom_station": "Paris | Rue de Bercy 88",
            "nom_amenageur": "TOTALENERGIES",
            "nom_operateur": "TOTALENERGIES",
            "puissance_nominale": 7.0,
            "latitude": 48.838615,
            "longitude": 2.3818836,
            "code_insee_commune": "75112",
            "date_mise_en_service": "2026-03-10T15:48:48+00:00",
            "source": "belib",
            "statut_actuel": "Occupé (en charge)",
            "num_arrondissement": 12.0,
        },
        {
            "id_pdc_itinerance": "FRV75EPX17252",
            "id_station_itinerance": "FRV75PPX1725",
            "nom_station": "Paris | boulevard Pereire 170",
            "nom_amenageur": "TOTALENERGIES",
            "nom_operateur": "TOTALENERGIES",
            "puissance_nominale": 7.0,
            "latitude": 48.882423,
            "longitude": 2.2922037,
            "code_insee_commune": "75117",
            "date_mise_en_service": "2021-11-23T00:00:00+00:00",
            "source": "belib",
            "statut_actuel": "Occupé (en charge)",
            "num_arrondissement": 17.0,
        },
        {
            "id_pdc_itinerance": "FRV75EPX18111",
            "id_station_itinerance": "FRV75PPX1811",
            "nom_station": "Paris | Rue Ravignan 1bis",
            "nom_amenageur": "TOTALENERGIES",
            "nom_operateur": "TOTALENERGIES",
            "puissance_nominale": 7.0,
            "latitude": 48.88508,
            "longitude": 2.3375535,
            "code_insee_commune": "75118",
            "date_mise_en_service": "2021-12-14T14:20:13+00:00",
            "source": "belib",
            "statut_actuel": "Disponible",
            "num_arrondissement": 18.0,
        },
        {
            "id_pdc_itinerance": "FRV75EPX19153",
            "id_station_itinerance": "FRV75PPX1915",
            "nom_station": "Paris | Avenue de la Porte d'Aubervilliers 10",
            "nom_amenageur": "TOTALENERGIES",
            "nom_operateur": "TOTALENERGIES",
            "puissance_nominale": 7.0,
            "latitude": 48.899334,
            "longitude": 2.371162,
            "code_insee_commune": "75119",
            "date_mise_en_service": "2021-11-23T16:20:49+00:00",
            "source": "belib",
            "statut_actuel": "Disponible",
            "num_arrondissement": 19.0,
        },
        {
            "id_pdc_itinerance": "FRV75EHBSAEPDA021",
            "id_station_itinerance": "FRV75PHBSAEPDA",
            "nom_station": "Paris | SAEMES Parking Porte d'Auteuil",
            "nom_amenageur": "TOTALENERGIES",
            "nom_operateur": "TOTALENERGIES",
            "puissance_nominale": 43.0,
            "latitude": 48.846973,
            "longitude": 2.2558389,
            "code_insee_commune": "75116",
            "date_mise_en_service": "2022-09-21T00:00:00+00:00",
            "source": "belib",
            "statut_actuel": "Disponible",
            "num_arrondissement": 16.0,
        },
    ]

    df = pd.DataFrame(data)

    df["date_mise_en_service"] = pd.to_datetime(
        df["date_mise_en_service"]
    )

    df["num_arrondissement"] = (
        df["num_arrondissement"]
        .astype(int)
    )
    return df


def test_collect_data_belib():
    df = collect_data("belib_stat", fromdate=None, todate=None, force=True)
    assert df is not None
    assert not df.empty
    assert "latitude" in df.columns
    assert "longitude" in df.columns
    assert "num_arrondissement" in df.columns

def test_collect_data_irve():
    df2 = collect_data("irve_conso", fromdate=None, todate=None, force=True)
    assert df2 is not None
    assert not df2.empty
    assert "latitude" in df2.columns
    assert "longitude" in df2.columns
    assert "num_arrondissement" in df2.columns


