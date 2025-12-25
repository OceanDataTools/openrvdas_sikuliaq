# OpenRVDAS Sikuliaq Code/Configs/Data

This repository contains code, configs and data related to running OpenRVDAS on the R/V Sikuliaq. Assuming your OpenRVDAS installation is in `/opt/openrvdas`, this repository should be cloned into `/opt` and symlinked as `/opt/openrvdas/local/sikuliaq`:

```
cd /opt

sudo git clone https://github.com/oceandatatools/openrvdas_sikuliaq
sudo chown -R rvdas /opt/openrvdas_sikuliaq

ln -s /opt/openrvdas_sikuliaq /opt/openrvdas/local/sikuliaq
```
