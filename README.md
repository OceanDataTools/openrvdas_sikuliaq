# OpenRVDAS Sikuliaq Code/Configs/Data

This repository contains code, configs and data related to running OpenRVDAS on the R/V Sikuliaq. Assuming your OpenRVDAS installation is in `/opt/openrvdas`, this repository should be cloned into `/opt` and symlinked as `/opt/openrvdas/local/sikuliaq`:

```
cd /opt

sudo git clone https://github.com/oceandatatools/openrvdas_sikuliaq
sudo chown -R rvdas /opt/openrvdas_sikuliaq

ln -s /opt/openrvdas_sikuliaq /opt/openrvdas/local/sikuliaq
```
## Writing to Grafana Live Displays
For the current Sikuliaq + CORIOLIX setup, CORIOLIX is handling all of the sensor logging and redistribution. The most relevant files in this directory are the ones that let the ship manage high-rate near-realtime sensor graphing via Grafana Live. Those are
- `utils/generate_grafana_live_stream.py` - Given a sensor id, interrogates the CORIOLIX API to retrieve parsing data and creates YAML for an OpenRVDAS logger that listens to that sensor's output, parses it, and feeds the parsed values to the specified Grafana Live instance. An example of such a file is in `test/grafana_live_cnss_cnav.yaml`.
- `utils/generate_grafana_cruise.py` - Interrogates the CORIOLIX API and generates a full OpenRVDAS cruise definition for creating Grafana Live streams for all sensors marked 'enabled'. An example definition is in `test/siluliaq_grafana.yaml`. 

Please see the [OpenRVDAS Grafana Live Setup](https://www.oceandatatools.org/openrvdas-docs/grafana_live/) document for instructions on setting up and configuring Grafana Live to work with OpenRVDAS loggers.
