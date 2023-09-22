# EE Download

![EEDL Logo](docs/source/_static/logo/logo_black.png)

This project aims to make downloading and processing of bulk data from Earth Engine feasible and simple. As of this
moment, it only supports image exports, but with a goal of supporting ImageCollections and tables at some point in the
future.

Many existing workflows exist for downloading areas small enough to fit into a single tile, but this tool
will automatically tile larger exports download the pieces, then reassemble them and optionally process the data
further using an arbitrary function (zonal statistics tools are included).

We are currently reworking this code base to extract it from another tool and documentation and examples will come soon.