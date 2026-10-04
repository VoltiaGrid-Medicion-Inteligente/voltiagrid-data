# Guía de Contribución — VoltaGrid Data

> Idioma: [🇺🇸 English](CONTRIBUTING.md) | [🇪🇸 Español](CONTRIBUTING.es.md)

> **Nota (P2):** esta guía se adaptó desde `voltiagrid-api`. El flujo genérico (ramas, commits, PRs) es final.
> Lo marcado `TODO(P2)` necesita el setup real de Spark/Airflow cuando existan los jobs.

Esta guía cubre el flujo completo: `git clone` → setup → rama → commit → PR → revisión → merge.
Combina [Conventional Commits](https://www.conventionalcommits.org/) (como el repo de referencia CoDecide) con el flujo **Jira (KAN)** del equipo.

---

## 0. Prerrequisitos

- Git, Python 3.12, Docker + Docker Compose.
- Cuenta de GitHub con acceso a `VoltiaGrid-Medicion-Inteligente/voltiagrid-data`.
- Cuenta de Jira. Tu email local de git **debe coincidir** con el de Jira, si no los Smart Commits no enlazan:
  ```bash
  git config user.name "Tu Nombre"
  git config user.email "tu@email-jira.com"
  ```

## 1. Clonar y setup (solo la primera vez)

```bash
# HTTPS (más simple)
git clone https://github.com/VoltiaGrid-Medicion-Inteligente/voltiagrid-data.git
cd voltiagrid-data

# o SSH (si usas llaves SSH)
# git clone git@github.com:VoltiaGrid-Medicion-Inteligente/voltiagrid-data.git
# cd voltiagrid-data

<!-- TODO(P2): reemplazar con la corrida local real: venv/Docker, spark-submit --mode dev, un DAG local. -->
python -m pytest tests/ -v
```

Reglas:

- **Nunca commitees `.env`.** Solo `.env.example` (sin secretos) va a git (CT-02).
- <!-- TODO(P2): documentar conexiones S3/RDS solo por variables de entorno, sin endpoints hardcodeados. -->

## 2. Estrategia de Ramas

```
main ────────────── rama estable, los PRs se mergean aquí (demo / release)
  ├── feature/KAN-12-descripcion-corta
  ├── fix/KAN-13-descripcion-corta
  ├── refactor/KAN-14-descripcion-corta
  ├── docs/KAN-15-descripcion-corta
  └── chore/KAN-16-descripcion-corta
```

### Convención de Nombres

```
<tipo>/KAN-<numero>-<descripcion-corta>
```

| Tipo | Cuándo usarlo | Ejemplo |
|------|---------------|---------|
| `feature/` | Nueva funcionalidad / historia | `feature/KAN-12-meter-consumer` |
| `fix/` | Corrección de error | `fix/KAN-13-login-redirect-loop` |
| `refactor/` | Reestructura sin cambio de comportamiento | `refactor/KAN-14-extract-meter-service` |
| `chore/` | Herramientas, dependencias, config, CI | `chore/KAN-16-upgrade-pytest` |
| `docs/` | Solo documentación | `docs/KAN-15-document-f3-simulator` |
| `test/` | Solo tests | `test/KAN-16-seed-hash-test` |

- Clave Jira en **MAYÚSCULAS** (`KAN-12`, no `kan-12`) para que Jira enlace rama → issue.
- Descripción en **kebab-case**, corta, en inglés de preferencia.
- Todas las ramas salen de `main` actualizado.

### Reglas

- **Nunca push directo a `main`.** Todo cambio por Pull Request.
- Cualquier commit directo a `main` será revertido/eliminado.
- Una rama por issue/tarea Jira. Si la tarea crece, divide el issue, no la rama.
- Mantén `main` en verde: haz pull antes de ramificar.

Crear una rama:

```bash
git checkout main
git pull origin main
git checkout -b feature/KAN-12-meter-consumer
```

## 3. Conventional Commits + Jira

### Formato

```
KAN-<numero> <tipo>(<alcance>): <descripcion>
```

- El prefijo `KAN-XX` mantiene la automatización Jira (rama/commit/PR enlazados).
- El resto sigue Conventional Commits.

### Tipos

| Tipo | Cuándo usarlo |
|------|---------------|
| `feat` | Nueva funcionalidad |
| `fix` | Corrección de error |
| `refactor` | Ni fix ni feature |
| `style` | Solo formato (sin cambio productivo) |
| `docs` | Solo documentación |
| `chore` | Build, deps, herramientas, CI |
| `test` | Agregar/modificar tests |
| `perf` | Mejora de rendimiento |

### Alcances (scopes, este repo)

Jobs: `jobs`, `clean`, `dedup`, `estimate`, `bands`, `losses`, `events`
Orquestación: `dags`, `airflow`
Transversales: `config`, `docker`, `ci`, `docs`, `deps`, `tests`

### Ejemplos (copia el estilo)

```
KAN-12 feat(clean): add null and range validation for raw readings
KAN-13 fix(dedup): keep highest sequence on duplicate meter interval
KAN-14 refactor(jobs): extract tariff band assignment helper
KAN-15 docs(dags): document daily close dag schedule
KAN-16 test(losses): cover suspect transformer rule
KAN-15 chore(config): add s3 paths override by env
```

### Reglas

- **Clave Jira primero, en MAYÚSCULAS** (`KAN-12`, no `kan-12`).
- **Descripción en inglés, imperativa, presente:** "add" no "added"/"adds". (Traducción con WordReference si dudas: "agregar" → "add", "corregir" → "fix", "mover" → "move").
- **Minúscula inicial, sin punto final**, concisa (<72 caracteres si es posible).
- Un cambio lógico por commit. Dos fixes no relacionados → dos commits.
- Commits pequeños. 20+ archivos en un commit → divídelo.

Buena división:

```
KAN-12 feat(jobs): add tariff band assignment
KAN-12 feat(dedup): wire dedup by sequence into clean job
```

Malo:

```
KAN-12 feat: add lots of stuff   # 35 archivos, 1200 adiciones
```

Corregir un mensaje antes de pushear:

```bash
git commit --amend -m "KAN-12 feat(jobs): correct message"
# si ya pusheaste a TU rama solamente:
git push --force-with-lease
```

### Smart Commits (automatización Jira)

Solo funcionan si `git config user.email` == email Jira:

```
KAN-12 #comment listo para revisión
KAN-12 #done
```

Úsalos en un commit aparte o en la descripción del PR — no los mezcles silenciosamente con cambios de código.

## 4. Flujo de Pull Request

1. Actualiza desde `main`, corre validaciones:

   ```bash
   git checkout feature/KAN-12-meter-consumer
   git pull --rebase origin main
   python -m pytest tests/ -v
   ```

2. Pushea y abre PR **contra `main**:

   ```bash
   git push -u origin feature/KAN-12-meter-consumer
   ```

3. Título del PR = mismo formato que el commit (clave Jira + Conventional):

   ```
   KAN-12 feat(clean): add null and range validation for raw readings
   ```

4. Descripción del PR (plantilla obligatoria):

   ```markdown
   ## Qué
   Descripción breve del cambio.

   ## Por qué
   Razón + issue Jira (ej. KAN-12).

   ## Cómo probar
   <!-- TODO(P2): completar la corrida local real. Esqueleto: -->
   1. spark-submit con --mode dev (o equivalente Docker)
   2. python -m pytest tests/ -v

   ## Capturas / evidencia (si aplica)
   ```

5. Espera revisión + CI en verde (lint, tests, secret scan). Responde comentarios con **nuevos commits**, no reescribas historia bajo revisión.
6. El mantenedor hace merge a `main` por PR revisado (squash por default, manteniendo `KAN-XX` en el título). `main` siempre debe quedar en verde y listo para demo.

### Checklist pre-PR

- [ ] Rama desde `main` actualizado, nombre `tipo/KAN-XX-kebab-case`.
- [ ] Commits `KAN-XX tipo(alcance): descripción en inglés imperativa`.
- [ ] `pytest` en verde local (o en Docker).
- [ ] Sin secretos/`.env`/credenciales en el diff (`git status`, `git diff --check`).
- [ ] El reproceso sigue idempotente si tocaste jobs (re-ejecutar no cambia nada).
- [ ] PR apunta a `main`, título + plantilla completos.

## 5. Qué hace que tu PR sea rechazado

- Push directo a `main`.
- Falta clave Jira o en minúscula (`kan-12`).
- Mensaje no-Conventional (`added stuff`, `fix style.`, oración capitalizada).
- Commit gigante, mezcla de temas, o regla de negocio sin test (toda RN-* necesita al menos un test automatizado).
- Secretos en código/imágenes, o endpoints/credenciales hardcodeadas.
