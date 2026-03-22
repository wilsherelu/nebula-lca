import type { LcaProcessTemplate } from "../model/node";

export const processLibrary: LcaProcessTemplate[] = [
  {
    id: "market-process",
    category: "Market Process",
    nodeKind: "market_process",
    mode: "normalized",
    processUuid: "market_process",
    name: "Market Process",
    location: "Global",
    referenceProduct: "Market Product",
    inputs: [
      {
        id: "market_in",
        flowUuid: "flow_market",
        name: "Market Input",
        unit: "kg",
        type: "technosphere",
      }
    ],
    outputs: [
      {
        id: "market_out",
        flowUuid: "flow_market",
        name: "Market Product",
        unit: "kg",
        type: "technosphere",
      }
    ]
  },
  {
    id: "continuous-reforming",
    category: "Unit Process",
    nodeKind: "unit_process",
    mode: "balanced",
    processUuid: "proc_continuous_reforming",
    name: "连续重整",
    location: "CN-SH",
    referenceProduct: "重整油",
    inputs: [
      {
        id: "in_naphtha",
        flowUuid: "flow_naphtha",
        name: "石脑油",
        unit: "kg",
        type: "technosphere",
      },
      {
        id: "in_steam",
        flowUuid: "flow_steam",
        name: "蒸汽",
        unit: "kg",
        type: "energy",
      },
      {
        id: "in_power",
        flowUuid: "flow_power",
        name: "电力",
        unit: "kWh",
        type: "energy",
      }
    ],
    outputs: [
      {
        id: "out_reformate",
        flowUuid: "flow_reformate",
        name: "重整油",
        unit: "kg",
        type: "technosphere",
      },
      {
        id: "out_h2",
        flowUuid: "flow_hydrogen",
        name: "氢气",
        unit: "kg",
        type: "technosphere",
      },
      {
        id: "em_co2",
        flowUuid: "flow_co2",
        name: "CO2",
        unit: "kg",
        type: "biosphere",
      }
    ]
  },
  {
    id: "distillation",
    category: "Unit Process",
    nodeKind: "unit_process",
    mode: "balanced",
    processUuid: "proc_distillation",
    name: "芳烃分馏",
    location: "CN-SH",
    referenceProduct: "混合芳烃",
    inputs: [
      {
        id: "in_reformate",
        flowUuid: "flow_reformate",
        name: "重整油",
        unit: "kg",
        type: "technosphere",
      }
    ],
    outputs: [
      {
        id: "out_aromatic",
        flowUuid: "flow_aromatic_mix",
        name: "混合芳烃",
        unit: "kg",
        type: "technosphere",
      }
    ]
  },
  {
    id: "pts_aromatic_section",
    category: "PTS",
    nodeKind: "pts_module",
    mode: "normalized",
    processUuid: "pts_aromatic_section",
    name: "芳烃联合单元 (PTS)",
    location: "CN-SH",
    referenceProduct: "对二甲苯",
    inputs: [
      {
        id: "pts_in_reformate",
        flowUuid: "flow_reformate",
        name: "重整油",
        unit: "kg",
        type: "technosphere",
      },
      {
        id: "pts_in_h2",
        flowUuid: "flow_hydrogen",
        name: "氢气",
        unit: "kg",
        type: "technosphere",
      }
    ],
    outputs: [
      {
        id: "pts_out_px",
        flowUuid: "flow_px",
        name: "对二甲苯",
        unit: "kg",
        type: "technosphere",
      },
      {
        id: "pts_out_toluene",
        flowUuid: "flow_toluene",
        name: "甲苯",
        unit: "kg",
        type: "technosphere",
      },
      {
        id: "pts_em_co2",
        flowUuid: "flow_co2",
        name: "CO2",
        unit: "kg",
        type: "biosphere",
      }
    ]
  },
  {
    id: "lci_grid_power",
    category: "LCI Dataset",
    nodeKind: "lci_dataset",
    mode: "normalized",
    lciRole: "provider",
    processUuid: "lci_grid_power",
    name: "背景电力 LCI",
    location: "CN",
    referenceProduct: "电力",
    inputs: [],
    outputs: [
      {
        id: "lci_out_power",
        flowUuid: "flow_power",
        name: "电力",
        unit: "kWh",
        type: "technosphere",
      },
      {
        id: "lci_em_co2",
        flowUuid: "flow_co2",
        name: "CO2",
        unit: "kg",
        type: "biosphere",
      }
    ]
  },
  {
    id: "lci_wastewater_treatment",
    category: "LCI Dataset",
    nodeKind: "lci_dataset",
    mode: "normalized",
    lciRole: "waste_sink",
    processUuid: "lci_wastewater_treatment",
    name: "污水处理 LCI",
    location: "CN",
    referenceProduct: "污水处理服务",
    inputs: [
      {
        id: "lci_in_wastewater",
        flowUuid: "flow_wastewater",
        name: "废水",
        unit: "kg",
        type: "technosphere",
      }
    ],
    outputs: []
  }
];

