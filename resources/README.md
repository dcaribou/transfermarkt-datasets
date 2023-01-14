Use the [mermaid-cli](https://github.com/mermaid-js/mermaid-cli) to refresh the image file

```shell
mmdc \
    -i diagram.mmd \
    --backgroundColor transparent \
    -o diagram.svg \
    --width 1040 \ # the width and height actually have no effect in how kaggle and data.world displays it
    --height 780
```
