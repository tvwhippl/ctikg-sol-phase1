# Pipeline Narrative

## Purpose of this document

This document explains the repository in normal technical language rather than code.

Its purpose is to describe:

- what the pipeline is
- what problem it is trying to solve
- what the main processing stages are
- what decisions the pipeline makes
- what tools and resources it uses
- what outputs it creates
- what an operator should and should not expect from it

This document is intended for:

- technical users who want a readable description of the repository
- non-coders who need to understand what the pipeline is doing
- downstream users who need to understand the handoff contract
- LLM-based tools that can read prose more easily than source code

It is a narrative companion to the runbooks, quickstarts, outputs contract, and script-level implementation.

## What this repository is

This repository is a topic-focused CTI article acquisition and packaging front-end for CTIKG / LLM4CTI-style downstream workflows.

At a high level, it takes a topic and turns it into a bounded, auditable article batch that can be used for downstream cybersecurity knowledge-graph experiments.

The repository performs the upstream work needed before graph extraction:

- topic definition or topic generation
- source collection and link queue construction
- lightweight scoring and helper flag generation
- topic selection and ranking
- bounded scraping of selected articles
- export and verification
- article-first downstream handoff

The repository is designed to support both local execution and SOL execution.

## What this repository is not

This repository is not the full CTIKG / LLM4CTI research platform.

It is not the final official downstream packaged graph-extraction pipeline.

It does not claim to solve topic-ranking robustness for every possible topic.

It does not guarantee that every topic can support large batches of relevant articles.

It does not exist to maximize article totals at any cost.

Instead, its value is in operationalization: it creates a reproducible, inspectable, provenance-preserving front-end that can hand clean article batches into downstream workflows.

## Why this pipeline exists

The project needed a practical way to move from a topic idea to a usable set of cybersecurity articles without relying on manual browsing and ad hoc copy-paste collection.

That required a pipeline that could do the following in a repeatable way:

- gather candidate links from curated CTI-relevant sources
- score and rank those links in a topic-aware way
- avoid scraping everything indiscriminately
- preserve evidence about what was selected and why
- export outputs that downstream tooling can actually use
- run both locally and on SOL

Over time, the project converged on a more honest and more useful goal.

The goal is not “collect the biggest possible number of articles.”

The goal is “collect the best relevant, provenance-preserving batch that a topic can honestly support.”

That design choice affects the whole repository. It is the reason the pipeline now prefers underfill over junk admission, manual downstream article export over unnecessary automation, and staged SOL execution over pushing every step into Slurm arrays.

## High-level pipeline flow

At the highest level, the pipeline works like this:

1. define a topic
2. generate or author a topic YAML file
3. gather candidate links from configured sources
4. normalize and flag the queue for lightweight triage
5. rank and select candidates for the topic
6. apply a quality gate so weak late-ranked junk is not admitted just to fill quotas
7. scrape the selected articles
8. export verified outputs
9. optionally create article-first handoff files for the current notebook workflow
10. preserve evidence and provenance for reuse, transfer, and review

In shorthand, the core flow is:

topic -> queue -> triage -> selection -> scrape -> export -> verify -> downstream handoff

## Main operating modes

The repository currently supports three practical operating modes.

### 1. Open-topic per-run workflow

This is the recommended workflow.

A single command creates an isolated run directory for one topic and writes all immediate outputs under that run directory.

This is the easiest mode to reason about and the cleanest mode for evidence retention.

### 2. SOL staged workflow with ranked-offset arrays

This is the recommended HPC pattern.

Topic generation and ranking happen once on the login node.

The ranked output is staged once.

Slurm arrays then operate only on the quality-gated ranked list for scraping, export, verification, and manifest creation.

This design avoids putting LLM topic generation into arrays and keeps the SOL path auditable and stable.

### 3. Legacy shared-path workflow

Older Make targets still exist and are still usable, but they write to shared fixed paths such as `data/`, `results/`, and `exports/`.

That makes them less suitable for repeated runs and less suitable for clean batch isolation.

They are still part of the repository for compatibility and background, but they are no longer the preferred path.

## Document roadmap

The rest of this document explains the repository in more detail:

- core inputs and configuration
- end-to-end open-topic workflow
- selection logic and quality gate behavior
- scraping and export logic
- SOL workflow
- downstream article-first handoff
- outputs and evidence
- operator expectations
- limitations and future-work boundaries

## Core inputs and configuration

The pipeline depends on a small number of input classes.

### Topic definition

A topic is represented as a YAML file.

That YAML file acts as the pipeline’s topic contract. It tells the selector what the topic is trying to capture, what to avoid, and how strict semantic fallback should be.

A current topic YAML may contain:

- `name`
- `include`
- `exclude`
- `fallback_anchors`
- `fallback_anchor_min_hits`
- `winners`

These fields play different roles.

#### `name`

The human-readable topic name.

This name is also used to derive a filesystem-safe topic name for run directories.

#### `include`

These are the strict topic phrases.

If the queue text clearly matches these phrases and avoids excluded content, those rows are treated as true topic matches.

This is the highest-confidence path into the ranked set.

#### `exclude`

These are phrases that represent known off-topic drift or categories of content the topic should avoid.

They are used to suppress obviously wrong candidates.

#### `fallback_anchors`

These are high-precision anchor substrings or short phrases used only during semantic fallback.

They exist because semantic similarity alone can still admit broad or misleading content. The anchor layer provides an additional topic-specific quality gate.

Typical examples include protocol names, product names, service names, or compound phrases that are hard to satisfy accidentally.

#### `fallback_anchor_min_hits`

This controls how many fallback anchors a semantically similar candidate must satisfy before it is admitted.

This is useful when a single anchor is still too weak and allows false positives.

The purpose of this field is not to maximize volume. Its purpose is to keep late-ranked junk out.

#### `winners`

Historically this represented a target number of selected items.

In the current open-topic path, the operational selection cap is controlled more directly by `SCRAPE_MAX`, while `winners` remains useful topic metadata and a fallback default.

### Source configuration

The repository uses a curated source configuration file, typically:

- `configs/sources/common.json`

This file defines where candidate links come from.

In practice, these sources include RSS and feed-like security content from vendor blogs, research teams, advisories, and related CTI-relevant sources.

The source list is not the same thing as topic control.

Source breadth determines where candidate links come from.
Topic logic determines which of those candidates are considered relevant enough to keep.

That distinction became important during the project, because adding more sources did not automatically solve topic relevance.

### Category and keyword resources

The repository also uses a broader keyword resource:

- `configs/Category_Keywords_Expanded.json`

This file supports the pre-ranking and queue-building stages.

It is part of the upstream link gathering logic, not the final topic gate.

### LLM provider configuration

Topic generation can use one of three practical provider modes.

#### Ollama

Used mainly for local workflows.

This is appropriate when running on a local machine with an available Ollama service.

#### OpenAI-compatible endpoint

Used for ASU Voyager and similar OpenAI-compatible APIs.

This is the main non-local LLM path used in the project closeout and SOL workflow.

In this mode, the important environment variables are:

- `LLM_PROVIDER=openai`
- `OPENAI_BASE_URL`
- `OPENAI_API_KEY`
- `LLM_MODEL`

On SOL, this mode is preferred for login-node topic generation.

#### dry-run

Used for deterministic smoke tests and offline workflow checks.

This avoids making a live LLM call and is useful when the goal is just to test pipeline behavior.

## Detailed open-topic workflow

The recommended repository path is the open-topic per-run workflow.

This path exists because it is cleaner, more reproducible, and easier to audit than the older shared-path workflow.

At a high level, one run creates one isolated run directory under:

- `runs/<SAFE_TOPIC>/<RUN_ID>/`

That directory becomes the container for the topic, queue, selection outputs, scrape outputs, exports, metadata, and run manifest.

### Step 1. The operator provides a topic

The operator starts with a topic string, for example:

- `Remote Code Execution`
- `SSH Credential Abuse and Lateral Movement`
- `JupyterHub and Open OnDemand Compromise`

The topic string is important, but it is not enough by itself.

The pipeline still needs to convert that topic into structured selection logic.

### Step 2. Topic YAML generation

The script:

- `scripts/gen_category_from_llm.py`

turns the topic string into a topic YAML file.

This step uses the chosen LLM provider unless the workflow is running in dry-run mode.

The generated topic YAML is the main interface between topic intent and the later selection logic.

The generation step now supports topic fields beyond just include/exclude phrases. It can also preserve:

- `fallback_anchors`
- `fallback_anchor_min_hits`

This matters because later topic quality depends not only on the topic wording, but also on how semantic fallback is controlled.

### Step 3. Candidate queue construction

Once the topic YAML exists, the pipeline gathers candidate links from configured sources.

This is done through:

- `scripts/pre_rank_links_v3.py`

The purpose of this stage is to build a candidate pool, not to make final topic decisions.

The queue stage gathers metadata such as titles, snippets, source domains, and lightweight ranking signals.

The output is a candidate queue snapshot.

### Step 4. Queue normalization and helper flags

The queue then passes through:

- `scripts/make_helper_flags.py`

This stage normalizes common column names and adds lightweight helper flags and quality indicators.

Examples include:

- reputation-like flags
- signal flags
- composite quality indicators

This stage is not the final topic selector either. It is a preparation stage that helps the selection logic operate on a cleaner queue.

### Step 5. Topic selection and ranking

The core selection logic lives in:

- `scripts/category_select.py`

This is one of the most important parts of the pipeline.

It takes the normalized queue and the topic YAML and decides what should count as topic-relevant enough to rank.

The current logic has multiple layers.

#### Strict topic matching

The first and most trusted path is strict matching.

If queue text matches include phrases and avoids exclude phrases, that candidate is admitted as a true topic match.

This is the cleanest path into the ranked set.

#### Semantic fallback

If strict matching is empty or insufficient, the selector can evaluate semantic similarity using a TF-IDF-based query similarity signal.

This is the `QuerySim` layer.

Historically, this layer could be too permissive.

That led to ranked outputs with weak or obviously off-topic late candidates.

#### Quality gate

The current repository now applies a stronger quality gate.

That gate includes:

- a positive minimum semantic similarity threshold
- no exclude-only fallback by default
- optional topic-defined `fallback_anchors`
- optional `fallback_anchor_min_hits`
- a structured selection summary that records what happened

This quality gate is one of the most important closeout changes in the repo.

Its role is not to maximize selected counts.

Its role is to stop the ranking process when the remaining candidates are not good enough.

That is a deliberate project decision.

Underfill is acceptable.
Junk admission is not.

### Step 6. Ranked and selected outputs

The selector writes multiple outputs.

#### `ranked.csv`

This is the full quality-gated ranked pool.

It is important for:

- pagination
- auditability
- understanding how deep the viable topic pool really is

#### `selected.csv`

This is the actual slice chosen for scraping.

It is determined by the ranking plus the operational cap and offset.

#### `selection_summary.json`

This is the audit record for selection behavior.

It explains whether the selector:

- filled from strict matches
- filled from fallback
- underfilled after the quality gate
- or stopped because no candidates passed the gate

This file is crucial because it makes the selector’s stopping logic inspectable.

Instead of silently returning weak late-ranked content, the pipeline now records why selection stopped.

That behavior is central to the current project closeout story.

### Step 7. Scraping

After selection, the pipeline scrapes only the selected URLs.

This is done through:

- `scripts/scrape_selected.py`

This step creates:

- a scrape log
- a JSONL article corpus
- local article artifacts
- scrape statistics

This stage is bounded by the selected slice.

That is a major design choice.

The repository does not scrape the whole queue just because it can.
It scrapes a bounded selected set.

### Step 8. Export and verification

The scraped corpus is then converted into downstream-friendly exports using:

- `scripts/export_ctikg_input.py`

This produces:

- `exports/ctikg_input.csv`
- `data/ctikg_docs_meta.json`

Then verification is performed using:

- `scripts/verify_export.py`

The purpose of verification is simple but important: confirm that the export is non-empty and structurally usable.

### Step 9. Manifest writing

The run then records provenance in:

- `manifest.json`

This file captures run-level context such as inputs, configuration, and execution details.

It is part of what makes the run auditable rather than ad hoc.

### Step 10. Manual article-first handoff

The repository does not automatically create the current notebook handoff artifacts as part of the main run.

Instead, article-first handoff is an explicit post-run step using:

- `scripts/export_llm4cti_articles.py`

This writes:

- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

This manual step exists by design.

It keeps the main run simpler and avoids unnecessary automation during closeout.

## Why the quality gate matters

The quality gate is not just a minor tuning detail.

It changes what the pipeline is allowed to claim.

Without a gate, a ranked pool can look larger than it really is because it includes late candidates that are only weakly related to the topic.

With the gate, the pipeline is allowed to underfill or stop.

That makes the resulting topic depth smaller, but more honest.

This matters for both handoff and presentation.

A smaller ranked pool that survives explicit topic controls is more useful than a larger pool padded with weak late-ranked content.


## Detailed SOL workflow

The SOL workflow follows the same overall logic as the open-topic workflow, but it changes where certain steps happen.

This change is intentional.

The purpose of the SOL pattern is to preserve reproducibility and provenance while avoiding unnecessary complexity inside Slurm arrays.

### Why the SOL workflow is staged

The repository does not treat SOL as a place to throw every step into a batch job.

Instead, it separates:

- login-node preparation work
- array-based repeated execution work

That separation exists for both practical and conceptual reasons.

Practically, LLM topic generation is not something that should be repeated independently inside many array tasks.

Conceptually, the array should operate on a fixed, already-decided ranked pool, not regenerate topic logic on the fly.

### Login-node stage

The login-node stage is where topic definition and ranking are established.

This stage typically performs the following tasks:

1. set topic and provider environment variables
2. generate the topic YAML once
3. build the queue once
4. normalize and flag the queue once
5. rank the queue once
6. write a staged selection summary once

The resulting staged directory contains the topic-level inputs that later array jobs depend on.

Typical staged files include:

- `runs/_stage/<SAFE_TOPIC>/config/topic.yaml`
- `runs/_stage/<SAFE_TOPIC>/queue/Links_Queue_sorted_flags.csv`
- `runs/_stage/<SAFE_TOPIC>/selection/ranked.csv`
- `runs/_stage/<SAFE_TOPIC>/selection/selection_summary.json`

The staged `selection_summary.json` is important.

It records whether the topic filled, underfilled, or stopped on quality before any scraping arrays start.

That means the operator can see topic viability before spending more compute or time.

### Why the selection summary belongs to the login-node stage

The selection summary is not a shard-level artifact.

It belongs to the ranking stage.

That is because it describes topic-level selection logic, not scrape-level execution behavior.

This distinction matters in the SOL workflow.

If an operator looks for `selection_summary.json` inside a shard directory and does not find it, that is not a failure.
The summary should be in the staged topic directory.

### Array stage

After login-node staging is complete, Slurm arrays operate on the staged ranked output.

Each task slices the ranked pool into a selected window and then performs the bounded scrape/export path.

The array stage is intentionally narrow.

Each shard only performs:

- slice from ranked output into `selected.csv`
- scrape the selected URLs
- export CTIKG inputs
- verify the export
- write the manifest

It does not regenerate the topic.
It does not rebuild the queue.
It does not rerun ranking.
It does not call the LLM provider for topic generation.

This design is central to the repository’s current SOL model.

### Why the array stage is intentionally narrow

Keeping the array narrow reduces ambiguity.

It means that if a shard fails, the operator can usually reason about the failure as one of:

- slice issue
- scrape issue
- export issue
- verify issue
- runtime / Slurm issue

rather than having to wonder whether topic generation or queue construction changed underneath the shard.

That is one of the main reasons the repository’s SOL path is considered more hardened than earlier project phases.

### Quality-gated arrays

Under the current design, arrays consume the already quality-gated ranked list.

That means the most important topic decision has already been made before arrays begin.

This matters because the array stage is not where topic quality is decided.
The array stage is where already-accepted ranked slices are processed.

If the stage-level ranking underfills, that is an honest signal that the topic does not support the requested depth cleanly.

The right response is usually to accept the underfill or adjust the topic, not to force the array to scrape weak late-ranked content.

## Outputs and evidence

The repository produces several classes of output.

These outputs do not all serve the same purpose.

Some are primarily for downstream handoff.
Some are for auditability.
Some are for debugging.
Some are legacy compatibility outputs.

### Open-topic per-run outputs

The current recommended workflow creates an isolated run directory:

- `runs/<SAFE_TOPIC>/<RUN_ID>/`

Within that directory, the main output families are:

#### Selection outputs

- `selection/ranked.csv`
- `selection/selected.csv`
- `selection/selection_summary.json`

These describe what the selector considered viable, what it actually chose for scraping, and why the selection stopped where it did.

#### Scrape outputs

- `scrape/scraped_corpus.jsonl`
- `scrape/scrape_log.csv`
- `scrape/scrape_stats.json`
- local HTML or artifact captures under `artifacts/`

These describe what was scraped, what succeeded or failed, and what supporting raw material was preserved.

#### Export outputs

- `exports/ctikg_input.csv`
- `data/ctikg_docs_meta.json`

These describe the downstream-ready sentence-level export and associated document metadata.

#### Provenance output

- `manifest.json`

This records run-level provenance and execution details.

### SOL staged outputs

The SOL staged path creates a topic-level staging directory that captures the pre-array state.

This staged output is evidence of what the array actually consumed.

The key staged outputs are:

- topic YAML
- normalized queue snapshot
- ranked topic pool
- selection summary

This is important because it separates topic viability from shard execution.

### SOL shard outputs

Each shard produces a smaller isolated run directory.

That shard output focuses on the slice that was actually processed.

Typical shard outputs include:

- `selection/selected.csv`
- scrape outputs
- export outputs
- `manifest.json`

The shard does not need to explain full topic-level selection behavior, because that explanation already exists in the staged selection summary.

### Why multiple output layers exist

The repository intentionally keeps multiple output layers because one file cannot serve every purpose.

For example:

- `selection_summary.json` explains topic-level decision logic
- `selected.csv` shows the scrape slice
- `scrape_log.csv` shows scrape outcomes
- `ctikg_input.csv` shows downstream sentence-level export
- `Articles.xlsx` supports the current notebook workflow
- `manifest.json` captures provenance

This layered design is part of what makes the handoff stronger than a single export CSV alone.

## Downstream handoff

The repository now uses an article-first downstream handoff model for the current notebook workflow.

This is one of the more important mature design choices in the repo.

### Why article-first handoff is primary

The downstream notebook workflow is article-first.

That means the most natural input is article-level content that can be chunked and processed downstream.

Because of that, the repository’s preferred downstream bridge is:

- `scrape/scraped_corpus.jsonl`
- plus the manually exported `llm4cti/` article files

This makes more sense than treating the sentence-level CSV as the only meaningful output.

### Manual post-run export

The repository does not automatically create notebook handoff files during the main pipeline run.

Instead, the operator explicitly runs:

- `scripts/export_llm4cti_articles.py`

This produces:

- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

This was kept manual on purpose.

The reason is that automatic generation would add pipeline complexity without being necessary for every run.

Some runs only need selection, scrape, export, and verification.
Not every run needs notebook-ready article handoff files immediately.

### Sentence-level export remains useful

Although article-first handoff is primary for the current notebook path, the sentence-level export still matters.

`ctikg_input.csv` remains useful for:

- spot inspection
- compatibility-oriented downstream checks
- simpler adapters
- quick sanity review of export contents

So the sentence-level export is not obsolete.
It is simply no longer the preferred primary bridge into the current notebook workflow.

### Compatibility proof versus official downstream pipeline

The repository also contains a downstream compatibility proof path.

This demonstrates that article-level handoff can feed a graph-style downstream extraction workflow.

However, that compatibility proof is not the same thing as the final official downstream packaged pipeline.

This boundary matters because it keeps the repo’s claims honest.

## Evidence retention and transfer

Evidence retention is a first-class concern in the repository.

The pipeline is not only trying to produce outputs.
It is trying to produce outputs that can be inspected, transferred, and defended later.

### What evidence is worth keeping

Typical evidence worth preserving includes:

- representative run directories
- `selection_summary.json`
- `ranked.csv`
- `selected.csv`
- scrape logs and scrape statistics
- article corpora
- export outputs
- article-first handoff files
- manifests
- staged SOL inputs
- `sacct` reports
- Slurm logs

### Why evidence should stay off generated-git state

The repository intentionally avoids committing generated run outputs to git.

That does not mean the outputs are unimportant.

It means they belong in evidence bundles, handoff artifacts, or preserved run directories rather than in the source repository itself.

This keeps the repo cleaner while still preserving the materials needed for closeout and transfer.


## Operator expectations

A good operator experience with this repository depends on understanding what kind of success the pipeline is designed to produce.

The pipeline is not optimized for “always return a full batch.”
It is optimized for “return the best defensible batch the topic can support.”

That means a successful run can take different forms.

### What success looks like

A successful run does not always mean a large selected count.

A successful run may mean:

- the topic produced a small but clearly relevant ranked pool
- the selector stopped because remaining candidates were too weak
- scraping succeeded on a bounded selected set
- exports were produced and verified
- provenance was preserved
- article-first handoff files can be created if needed

Under the current design, a topic that yields 2 or 3 strong articles may be more successful than a topic that yields 25 weak ones.

### What underfill means

Underfill should not automatically be interpreted as a failure.

In the current repository, underfill often means one of two things:

- the topic is real but the available source pool only supports a small clean batch
- the topic wording, anchor strategy, or source environment is not strong enough to support deeper clean ranking

In both cases, underfill can still be an honest and useful result.

The current repository deliberately treats that as preferable to late-ranked junk admission.

### What an empty ranked or selected set means

An empty result is not automatically a broken pipeline.

It can mean:

- no strict topic matches were found
- no semantic fallback candidates passed the quality gate
- fallback anchors were too weak, too strict, or too mismatched to the queue
- the topic is simply not viable against the current source pool

That is one of the reasons the pipeline now writes `selection_summary.json`.

The summary exists to distinguish:

- “topic not viable under the quality gate”
from
- “pipeline malfunction”

### What a successful handoff means

A successful handoff means the repository produced a reusable topic run with preserved evidence and outputs that downstream tooling can consume.

That may include:

- sentence-level export
- article-level handoff files
- a manifest
- scrape logs
- selection records
- staged inputs for SOL

The pipeline is successful when these materials are good enough to support downstream work, troubleshooting, reuse, and transfer.

## How to interpret different run outcomes

The repository now makes a clearer distinction between different run outcomes.

### Outcome type 1: Strict topic success

This is the cleanest case.

The topic’s include/exclude logic finds high-confidence topic matches directly.

This usually means the topic is well aligned to the current source pool.

### Outcome type 2: Semantic fallback with quality gate

This means strict matching alone was not enough, but semantic similarity plus fallback anchors still yielded a usable topic pool.

This is acceptable when the resulting pool remains relevant.

The anchor layer exists to ensure semantic fallback remains topic-controlled rather than drifting into broad security content.

### Outcome type 3: Underfilled but valid

This means the selector found some valid candidates, but not enough to reach the requested cap.

This is a normal and acceptable outcome under the current design.

It often reflects real topic scarcity rather than pipeline failure.

### Outcome type 4: Stopped on quality

This means the selector intentionally returned no viable ranked pool after applying the quality gate.

This is an important success mode for the selector itself, even if it is not a successful topic run.

It means the pipeline avoided a false success.

### Outcome type 5: Execution failure

This is different from topic scarcity.

Execution failure means something in the run path itself failed, for example:

- scraping problems
- export problems
- verification problems
- Slurm/runtime failures
- provider/authentication failures

These should be diagnosed from logs, manifests, and run artifacts rather than inferred from selected counts alone.

## Limitations and future-work boundaries

The repository is stronger now than it was earlier in the project, but several real limitations remain.

These limitations matter because they define what should still be treated as unfinished or future work.

### Ranking robustness is improved, not solved

The selector is now better controlled than it was before.

However, broader topic-ranking robustness is still not solved as a research problem.

The current quality gate reduces false positives and prevents silent junk admission, but it does not guarantee that every topic can produce a clean, sufficiently deep ranked pool.

This remains a true limitation.

### Source breadth is not the same as topic depth

Adding more sources can increase the size of the candidate queue.

That does not automatically increase the number of truly relevant topic matches.

This was an important lesson in the project.

A large candidate queue can still collapse into a small clean topic pool once quality controls are applied.

### The repository is not the final downstream graph pipeline

The repository now supports article-first handoff, sentence-level export, and even a downstream compatibility proof.

That still does not make it the final official graph-extraction platform.

The official downstream packaged path remains a separate future-work boundary.

### Manual article export remains a deliberate choice

The article-first handoff export is still manual by design.

This is not because the step is impossible to automate.

It is because the repository closeout prioritized operational clarity and bounded complexity over additional automation.

That may be revisited later, but it remains the right choice for the current phase.

### Multi-topic evidence should remain bounded

The repository now supports more honest topic probing.

That does not mean it should restart a large multi-topic scale campaign.

Additional topic evidence should remain bounded, relevance-first, and provenance-preserving.

That is the correct continuation of the project’s mature direction.

### Some failures will remain unrecoverable

Not every historical failure can be fully reconstructed later.

For example, some shard-level failures may lack enough retained logs or artifacts to support a confident root-cause claim.

The correct response is to classify those honestly, not to force a stronger conclusion than the evidence supports.

## What this repository should and should not be used to claim

This repository can support some claims well, and other claims only weakly or not at all.

### Claims it can support well

It can support claims such as:

- the article acquisition and packaging workflow was operationalized
- the pipeline can run locally and on SOL
- topic selection is now more tightly controlled than before
- the repository preserves better provenance and evidence than earlier project phases
- article-first downstream handoff is real and usable
- bounded topic batches can be produced honestly even when topics underfill

### Claims it should not be used to support

It should not be used to claim:

- the full CTIKG / LLM4CTI platform is complete
- topic-ranking robustness is solved in general
- every topic can scale to large clean batches
- the downstream compatibility proof is the final official packaged graph pipeline
- the pipeline should always maximize selected counts

This distinction matters for handoff, documentation, and presentation work.

## Practical mental model for non-coders

For a non-coder, the easiest way to understand the repository is to think of it as a controlled funnel.

### Stage 1: define the topic

You describe what kind of CTI article you are looking for.

### Stage 2: gather possible articles

The repository collects many possible candidates from configured sources.

### Stage 3: filter and rank them

The repository tries to keep only candidates that match the topic well enough.

This is where the most important logic lives.

### Stage 4: stop when quality drops

The repository is now designed to stop when the remaining articles are too weak, instead of pretending it found a bigger clean batch than it really did.

### Stage 5: scrape and export the good ones

Only the selected articles are scraped and exported.

### Stage 6: hand them off downstream

The output can then be used for later notebook or graph-oriented workflows.

This mental model is simpler than the code, but it captures the most important behavior correctly.

## Closing summary

This repository is best understood as a hardened, topic-focused article acquisition and packaging layer.

Its main accomplishment is not that it finished the entire downstream research platform.

Its main accomplishment is that it made the upstream process operational, inspectable, portable, and more honest.

The current design favors:

- bounded selection over indiscriminate scraping
- quality gates over inflated ranked counts
- provenance over ad hoc execution
- article-first handoff over over-reliance on a single CSV
- transferability over one-off manual collection

That makes the repository useful in three ways:

- as an operational tool
- as a handoff asset
- as a readable technical foundation for future downstream work

