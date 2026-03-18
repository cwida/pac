# Membership Inference Attacks Against PAC Privacy

## Setup

All attacks follow the same scenario:

- **Goal:** Determine whether a specific *target* person is in the database (membership inference).
- **Attacker knowledge:** The attacker knows the full background data and the target's value (worst-case adversary).
- **Database:** 1,000 users with `acctbal` drawn from [1, 10,000]. The target is an outlier (`acctbal = 50,000`, i.e. 5-10x the max normal value).
- **PAC config:** MI budget = 1/128 (the tightest privacy setting).
- **PAC theoretical bound:** At MI=1/128, no attacker should exceed ~53% accuracy (vs. 50% random guess).

### Methodology

Each attack is a repeated experiment. Per trial:

1. Build a fresh database with the background population.
2. Run the trial twice: once with the target inserted ("in"), once without ("out").
3. The attacker observes the PAC-noised query result and guesses "in" or "out" using a decision rule (e.g., threshold test).

This is repeated for N trials (200-1000 depending on the script). **Accuracy** is the fraction of correct guesses across all 2N decisions (N "in" + N "out"):

```
accuracy = (true positives + true negatives) / (2 * N_trials)
```

A 50% accuracy means the attacker is no better than a coin flip. The PAC theoretical bound at MI=1/128 is ~53%, meaning no attacker should reliably exceed this.

| Script | Trials per condition |
|---|---|
| basic_threshold.sh | 500 |
| filter_and_mi.sh | 200 |
| small_filter_deepdive.sh | 500 |
| null_sidechannel.sh | 500 |
| definitive_stats.sh | 1,000 |
| cross_query_composition.sh | 500 |
| maximum_damage.sh | 200 |

### Concrete Example

Imagine a hospital database with 1,000 patients. Eve (the attacker) wants to know whether Alice visited the hospital. Eve knows:

- The other 999 patients each spent between $1 and $10,000 on treatment.
- Alice, if present, spent $50,000 (an outlier -- e.g., a rare surgery).
- The total spending of the 999 other patients is exactly $4,876,582.

Eve queries: `SELECT SUM(treatment_cost) FROM patients` and gets back a PAC-noised result.

- If Alice is **in** the database, the true sum is $4,926,582.
- If Alice is **out**, the true sum is $4,876,582.
- The difference is $50,000 (Alice's contribution).

Eve picks the midpoint ($4,901,582) as her threshold: if the noised result is above it, she guesses "Alice is in"; below it, "Alice is out."

**The question is:** does PAC add enough noise to make Eve's guess no better than a coin flip?

Each attack script automates this experiment hundreds of times with different random seeds, measuring how often Eve guesses correctly. If accuracy stays near 50%, PAC is working. If it climbs toward 53% or beyond, the attacker is extracting real signal.

---

## Attack 1: Simple Threshold (`basic_threshold.sh`)

**Idea:** Query `SUM(acctbal)` and check if the noised result is above or below the midpoint between the "target in" and "target out" expected values.

**Why it works in theory:** If the target (acctbal=50,000) is present, the true sum is 50,000 higher. The attacker picks a threshold halfway between the two hypotheses.

**Result:** ~50% accuracy. PAC noise completely drowns out the 50,000 signal.

---

## Attack 2: Narrowing + Budget + Averaging (`filter_and_mi.sh`)

Three ideas tested:

### 2A: WHERE filter to isolate the target
Instead of `SUM(acctbal)` over all 1,000 users, query only `WHERE user_id <= 5`. Now the target's 50,000 is a huge fraction of a tiny group sum (~25,000 background), making the signal-to-noise ratio much better.

**Result:** Accuracy climbs to ~54-55% for very small filters.

### 2B: Increase MI budget
At MI=1/4 instead of 1/128, PAC adds far less noise.

**Result:** Accuracy increases as expected, tracking the theoretical bounds.

### 2C: Average multiple independent queries
Run the same query many times (different seeds), average the results. Noise cancels out, signal doesn't.

**Result:** Marginal improvement. Averaging K queries reduces noise std by sqrt(K).

---

## Attack 3: Statistical Deep-Dive (`small_filter_deepdive.sh`)

**Idea:** The filter<=5 result (54.2%) looked like it might exceed the PAC bound. Run 500 trials with z-tests to check statistical significance.

Also pushes to extreme filters: <=2 and <=1 (near-singleton groups).

**Key discovery:** PAC returns NULL for very sparse groups. This becomes the basis for Attack 4.

---

## Attack 4: NULL Side-Channel (`null_sidechannel.sh`)

**Idea:** PAC probabilistically returns NULL based on how many protected units contribute to an aggregate. Fewer PUs in the result = higher chance of NULL.

When the target is *absent*, the group has fewer PUs, so NULLs are *more frequent*. The attacker doesn't even need to analyze the noised value -- just observe whether the query returned NULL or a number.

**Decision rule:**
1. NULL returned -> guess "target is out"
2. Value returned -> guess "target is in" (or fall back to threshold test)

**Result:** The NULL rate difference is measurable for small filter sizes, giving a pure side-channel that is independent of the noise magnitude.

---

## Attack 5: High-Confidence Final Numbers (`definitive_stats.sh`)

Runs 1,000 trials on the best attack vectors (full population, filter<=2, filter<=5) and reports z-scores against both 50% (random) and 53% (PAC bound).

**Purpose:** Definitively answer whether any single-query attack exceeds the theoretical bound, with proper statistical significance testing.

---

## Attack 6: Cross-Query Composition (`cross_query_composition.sh`)

**Idea:** Instead of repeating the *same* query, run *different* queries and combine evidence.

### 6A: Multiple aggregates (SUM + COUNT + AVG)
Query SUM, COUNT, and AVG on the same data. Each gives a different view of the target's presence.

**Caveat:** Within a single query session, these share the same secret world (same noise seed), so they are correlated, not independent.

### 6B: Disjoint partition queries
Split user_id into 5 ranges. The target (id=0) appears in partitions 1 and 5. Partitions 2-4 serve as controls (no target signal, only noise). Combine evidence from target-containing partitions and subtract control noise.

### 6C: 10 independent queries on a small filter
Each query uses a different `pac_seed`, giving independent noise. Accuracy improves progressively as more queries are composed.

**Result:** Composing 10 queries pushes accuracy to ~63%.

### 6D: COUNT on narrow filters
Target adds exactly 1 to count. Tests whether PAC noise hides a signal of magnitude 1 for small groups.

---

## Attack 7: Maximum Damage (`maximum_damage.sh`)

Combines every advantage discovered in earlier attacks:

### 7.1: Extreme outlier + tiny filter + 20 queries
- Target value pushed to 999,999 (200x the mean)
- Filter <= 3 (tiny group)
- 20 composed queries with different seeds
- Shows a progressive accuracy curve as queries accumulate

### 7.2: Variance-based classifier
Exploits that `std(in) >> std(out)` for small filters. The target's large value doesn't just shift the mean -- it inflates the noise *variance* (because PAC noise scales with sensitivity). The attacker classifies based on how far the result deviates from the background sum, regardless of direction.

**This is the most devastating attack.** Measured: std(in) ~ 7,300,000 vs std(out) ~ 94,000. A threshold on |deviation| > 200K gives 96% accuracy; > 500K gives 100%.

### 7.3: Larger database (10K users) + extreme outlier
Tests whether the attacks scale to bigger populations.

### 7.4: 10K users + 10 composed queries
Majority vote over 10 independent queries on 10K users with a massive outlier.

---

## Summary of Findings

Results from `maximum_damage_quick.sh` (30 trials per condition, MI=1/128):

| Attack | Accuracy | vs. PAC Bound (~53%) |
|---|---|---|
| Single query, full population (Attack 1) | ~50% | Within bound |
| Single query, filter<=3, target=999,999 | **~80%** | Far exceeds bound |
| Variance classifier (|dev| > 200K) | **96%** | Breaks it completely |
| Variance classifier (|dev| > 500K) | **100%** | Trivial to distinguish |
| 10K users, filter<=2, target=9,999,999 | **~78%** | Far exceeds bound |
| 5 composed queries, majority vote (1K users) | **~68%** | Exceeds bound |
| 5 composed queries, majority vote (10K users) | **~67%** | Exceeds bound |

Note: composing more queries via averaging actually *decreases* accuracy (80% -> 72%) because averaging pulls extreme noised values toward the center. Majority vote is more robust than averaging.

### Key Takeaways

1. **Full-population single-query privacy holds.** For the full population with a moderate outlier, PAC noise drowns the signal -- accuracy stays near 50%.
2. **Small filters + extreme outliers break privacy.** A `WHERE user_id <= 3` filter combined with a target value of 999,999 (200x the mean) achieves ~80% accuracy on a single query -- far above the 53% theoretical bound.
3. **The variance channel is the strongest attack.** PAC noise variance scales with the target's value. When the target is present, `std(in)` is orders of magnitude larger than `std(out)` (e.g. 7M vs 94K). A simple "is the deviation large?" test reaches 96-100% accuracy.
4. **NULL returns are a side-channel.** The probability of NULL depends on group size, leaking membership information independent of noise.
5. **Query composition helps but isn't the main threat.** Averaging multiple queries actually hurts when the signal is already strong. The single-query variance attack is more damaging than multi-query composition.