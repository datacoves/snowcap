# Why Snowcap

Picking a tool to manage Snowflake permissions at scale is harder than it should be. The options range from "too general" to "not maintained" to "works fine until you have a thousand roles." Here's what we found.

## The Contenders

| | Snowcap | Snowflake DCM | Terraform | SnowDDL | Permifrost |
| --- | --- | --- | --- | --- | --- |
| **Snowflake-native** | :white_check_mark: | :white_check_mark: | :x: | :white_check_mark: | :white_check_mark: |
| **No state file** | :white_check_mark: | :warning: in-account history | :x: | :white_check_mark: | :white_check_mark: |
| **YAML + Python** | :white_check_mark: | :x: SQL only | HCL only | YAML only | YAML only |
| **Speed** | 50%+ faster than alternatives | Runs in-account | Baseline | Fast | Slow out of the box (serial) |
| **Object creation** | :white_check_mark: 60+ resource types | :white_check_mark: Most types | :white_check_mark: Most types | :white_check_mark: Most types | :x: Grants only |
| **Templating / loops** | :white_check_mark: | :white_check_mark: Jinja | :white_check_mark: | Limited | :x: |
| **Export existing resources** | :white_check_mark: | :x: | Import only | :x: | :x: |
| **Actively maintained** | :white_check_mark: Datacoves-owned | :white_check_mark: Snowflake-owned | :white_check_mark: Snowflake-owned | :warning: Single maintainer | :warning: Slow-moving, infrequent releases |
| **Enterprise RBAC support** | :white_check_mark: | :white_check_mark: | :white_check_mark: | Limited | :white_check_mark: |

## Why Not Terraform?

Snowflake maintains an official Terraform provider, and it's technically capable. But Terraform was designed for multi-cloud infrastructure, not for teams whose entire IaC scope is a single Snowflake account.

The state file is the core problem. Terraform tracks your infrastructure in a local or remote file, and the moment someone makes a change directly in Snowflake, that file is wrong. In data teams, out-of-band changes happen constantly. An admin creates a warehouse. Someone adds a user through the UI. The state drifts, reconciling it is tedious, and the risk of accidentally reverting real changes is never zero.

HCL also just isn't where data engineers live. YAML and Python are. Adding Terraform to the stack means adding a tool most of the team won't touch, which usually means it becomes one person's job and knowledge. That's a bad place to end up with infrastructure tooling.

## Why Not SnowDDL?

SnowDDL had one thing going for it that the other options didn't: it was parallelized from the ground up. That matters more than it sounds. At enterprise scale, tools that query Snowflake serially can take two hours or more to run. SnowDDL doesn't have that problem.

What it does have is a single maintainer and limited community adoption. For something you're building production deployments on, that's a real concern. If the project goes quiet, you're owning a codebase you didn't write.

The RBAC model is also more opinionated than it appears. When you need tier-2 roles granting to other tier-2 roles (which comes up constantly in large accounts), SnowDDL pushes back. There's no templating to speak of, so building out role structures for 40 country-specific environments means a lot of copy-paste. And there's no export CLI, so migrating an existing account requires writing config by hand.

## Why Not Permifrost?

We have used Permifrost for years. It was a reasonable choice when we adopted it: YAML-based, created by GitLab, approachable for anyone who understands Snowflake's permission model. The problem is where it stopped.

Permifrost only manages grants. It can't create a warehouse, a role, a schema, or a database. Everything has to exist before Permifrost can touch it, which means running a separate provisioning script first. That script has to be maintained separately, and when something breaks, you're debugging two parts instead of one.

It also hasn't kept up with Snowflake. Streamlit apps, dynamic tables, and iceberg tables have no support. There isn't even an open issue requesting them. The `references` privilege isn't there either. We submitted pull requests for parallelization and quoted identifier handling. Both were rejected. The project still ships the occasional release, but its scope hasn't grown in years, and enterprise Snowflake deployments have moved well past what it covers.

Out of the box, Permifrost runs serially. Every query to Snowflake happens one at a time. At a large enough account, that translates to two hours or more for a single apply run. We parallelized our own fork to get that down, but it's still a workaround on top of an architecture that wasn't designed for this scale. Add a 7,000-line YAML file with no templating, and similar role structures for different regions or environments end up copy-pasted and slowly diverging. It works until it doesn't.

## Why Not Snowflake DCM?

DCM (Database Change Management) Projects are Snowflake's own declarative deployment feature, and of every option here it's the one most architecturally similar to Snowcap. Both are Snowflake-native, both are declarative—you describe the end state and the tool generates the SQL to get there—and neither relies on an external, drift-prone state file. If you're weighing the two, the differences come down to ownership, authoring experience, and onboarding.

DCM is first-party, which is a real advantage: it's built into the platform and moves with Snowflake's roadmap. The flip side is that you're bound to that roadmap. DCM is newer and still maturing through preview, and when you hit a gap you wait for Snowflake to close it. Snowcap is open source and Datacoves-maintained, so when Snowflake ships something new, we can add support for it directly.

Authoring is the other major difference. DCM expects SQL definition files built around the `DEFINE` statement plus a `manifest.yml`. Snowcap meets data engineers where they already are, in YAML or Python. Both tools can stamp out repeated resources from a list, but the mechanism differs: DCM uses raw Jinja `{% for %}` loops and macros inside the definition files, so you hand-write the control flow that emits the SQL. Snowcap exposes a declarative `for_each` on the resource itself, fed by `vars`—you say "apply this resource across this list" rather than writing the loop. Both are equally capable; Snowcap's is more constrained and more readable for the common "create N of these" case.

DCM also keeps deployment artifacts and version history inside the account, in a DCM Project Object. That's useful for auditing, but it's still state that lives somewhere and has to be managed. Snowcap holds no state at all—it reads the live account on every run, so changes made outside the tool are simply picked up on the next plan.

Finally, onboarding an existing account. DCM has no way to reverse-engineer what you already have, so adopting it on a brownfield account means writing definitions by hand. `snowcap export` generates config from your current account, so you start from where you are.

## What Snowcap Gets Right

Snowcap is maintained by Datacoves and extended based on what we've seen running real deployments in production.

**Speed.** It runs 50%+ faster than Terraform and Permifrost. That's not a benchmark we invented; it's what you see when you run it against an account with over a thousand roles.

**No state file.** Snowcap queries Snowflake directly on each run. There's nothing to reconcile, no drift to worry about, and no risk from changes made outside the tool. Someone creates a warehouse manually and Snowcap just knows about it on the next run.

**60+ resource types.** Warehouses, databases, schemas, roles, grants, users, dynamic tables, hybrid tables, stages, pipes, streams, tasks, stored procedures, integrations, policies. One tool for all of it.

**YAML or Python.** Teams that want declarative config can use YAML. Teams that want to generate resources programmatically can use the Python API. Both are supported.

**Templating.** Define a role pattern once and apply it across a list. Country-based role structures, environment variants, business unit splits—all of it generates from a single definition instead of being copy-pasted and allowed to drift.

**Export CLI.** If you have an existing Snowflake account, `snowcap export` generates config from what's already there. You don't have to start from scratch.

We use it on every Datacoves deployment. When Snowflake ships something new, we can add support for it.
