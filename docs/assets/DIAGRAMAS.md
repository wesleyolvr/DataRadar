# Diagramas do DataRadar

## Arquitetura (`arquitetura_dataradar`)

| Arquivo | Função |
|---------|--------|
| `arquitetura_dataradar.mmd` | Fonte **Mermaid** versionada (inclui Job → `medallion_pipeline.py` e `%run`). Edite este arquivo para mudar o fluxo. |
| `arquitetura_dataradar.png` | PNG gerado para README e docs (GitHub). |
| `arquitetura_dataradar.excalidraw` | Opcional: desenho manual no [Excalidraw](https://excalidraw.com/). Se estiver desatualizado em relação ao `.mmd`, abra o `.excalidraw`, importe o PNG novo como imagem de referência e ajuste os elementos. |

### Regenerar o PNG

Na raiz do repositório (requer Node/npm):

```bash
npx @mermaid-js/mermaid-cli@11.4.0 -i docs/assets/arquitetura_dataradar.mmd -o docs/assets/arquitetura_dataradar.png -w 3200 -H 1800
```

O `.mmd` já define **fonte ~28px** e **espaçamento** maior entre nós; a largura/altura altas no comando evitam o texto pixelado ao zoom.

Outros PNGs (`pipeline.png`, `ai_insights.png`, `agendamento_dataradar.png`) continuam sendo exports manuais ou de outras fontes; este fluxo automatiza só o diagrama de **arquitetura**.
