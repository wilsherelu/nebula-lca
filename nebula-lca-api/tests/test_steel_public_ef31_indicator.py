from app.services import provider_ef31


def test_bundled_ef31_indicator_identity_uses_exact_index_fields(tmp_path, monkeypatch):
    index = tmp_path / "indicator_index.csv"
    index.write_text(
        "indicator_index,method_en,indicator_en,LCIA_unit\n"
        "0,Climate change,Global Warming Potential (GWP100),kg CO2-Eq\n"
        "1,Climate change-Fossil,Global Warming Potential (GWP100),kg CO2-Eq\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(provider_ef31, "_INDICATOR_METHOD_METADATA", index)

    records, receipt = provider_ef31._indicator_unit_metadata([tmp_path])

    assert records["climate change|global warming potential (gwp100)"]["unit"] == "kg CO2-Eq"
    assert records["climate change-fossil|global warming potential (gwp100)"]["unit"] == "kg CO2-Eq"
    assert receipt["indicator_count"] == 2
    assert receipt["identity_runtime_indicator_sha256"] == receipt["sha256"]
