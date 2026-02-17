# ❓ how to populate the data folder
This folder contains all raw and prepared data from the project. Being a [dvc](https://dvc.org/) repository, Github does not store the data itself but a reference to it stored in Cloudflare R2.

Pull all data (no credentials needed — public remote):

```sh
dvc pull
```

See the [developer guide](../docs/developer-guide.md#data-storage) for more details, including how to push data to the remote.
