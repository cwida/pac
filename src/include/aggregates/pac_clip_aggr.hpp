//
// pac_clip_aggr.hpp: Shared constants, helpers, and bind data for clip aggregates
// (pac_clip_sum, pac_clip_min, pac_clip_max)
//
#ifndef PAC_CLIP_AGGR_HPP
#define PAC_CLIP_AGGR_HPP

#include "duckdb.hpp"
#include "pac_aggregate.hpp"
#include <cmath>
#include <climits>

namespace duckdb {

// ============================================================================
// Shared clip aggregate constants
// ============================================================================
constexpr int CLIP_NUM_LEVELS = 62;    // 62 levels × 2-bit bands covers full 128-bit
constexpr int CLIP_NUM_LEVELS_64 = 30; // 30 levels covers 64-bit (max_level = 29)
constexpr int CLIP_LEVEL_SHIFT = 2;    // 2^2 = 4x per level

// Float/double → int64 scale factors (powers of 2 for exact FP arithmetic)
constexpr int CLIP_FLOAT_SHIFT = 20;                                              // 2^20 ≈ 1M
constexpr int CLIP_DOUBLE_SHIFT = 27;                                             // 2^27 ≈ 100M
constexpr double CLIP_FLOAT_SCALE = static_cast<double>(1 << CLIP_FLOAT_SHIFT);   // 1048576.0
constexpr double CLIP_DOUBLE_SCALE = static_cast<double>(1 << CLIP_DOUBLE_SHIFT); // 134217728.0

// ============================================================================
// Scale float/double to int64 with branchless clamping
// ============================================================================
template <typename FLOAT_TYPE, int SHIFT>
static inline int64_t ScaleFloatToInt64(FLOAT_TYPE value) {
	FLOAT_TYPE scale = static_cast<FLOAT_TYPE>(1 << SHIFT);
	FLOAT_TYPE scaled = value * scale;
	scaled = std::max(scaled, static_cast<FLOAT_TYPE>(INT64_MIN));
	scaled = std::min(scaled, static_cast<FLOAT_TYPE>(INT64_MAX));
	return static_cast<int64_t>(scaled);
}

// ============================================================================
// Birthday-paradox distinct-count estimation from 64-bit bitmap
// ============================================================================
static inline int ClipEstimateDistinct(uint64_t bitmap) {
	int k = pac_popcount64(bitmap);
	if (k >= 64) {
		return 256; // saturated — could be any large number
	}
	if (k == 0) {
		return 0;
	}
	// n ≈ -64 * ln(1 - k/64)
	return static_cast<int>(-64.0 * std::log(1.0 - k / 64.0));
}

// ============================================================================
// Shared boundary helpers for clip outlier elimination
// ============================================================================

// Scan levels to find first and last with sufficient distinct-PU support.
// bitmap_offset: index of the bitmap uint64_t within a level buffer
// (17 for sum, PCMM_SWAR=8 for min_max).
static inline void ClipFindSupportedRange(uint64_t *const *levels, int max_level_used, int bitmap_offset, int threshold,
                                          int &first_supported, int &last_supported) {
	first_supported = -1;
	last_supported = -1;
	for (int k = 0; k <= max_level_used; k++) {
		if (levels[k] && ClipEstimateDistinct(levels[k][bitmap_offset]) >= threshold) {
			if (first_supported < 0) {
				first_supported = k;
			}
			last_supported = k;
		}
	}
}

// Given level k and the supported range, return the effective level to use
// for scale computation, or -1 to skip this level entirely.
// clip_scale=false (default): omit unsupported prefix/suffix levels.
// clip_scale=true: scale them to nearest supported boundary.
static inline int ClipEffectiveLevel(int k, int first_supported, int last_supported, bool clip_scale) {
	if (first_supported < 0) {
		return -1; // no supported levels at all
	}
	if (k < first_supported) {
		return clip_scale ? first_supported : -1; // prefix: scale or omit
	}
	if (k > last_supported) {
		return clip_scale ? last_supported : -1; // suffix: scale or omit
	}
	return k; // interior or supported: actual level
}

// ============================================================================
// Shared bind data for all clip aggregates
// ============================================================================
struct PacClipBindData : public PacBindData {
	int clip_support_threshold; // levels with fewer estimated distinct contributors are clipped
	double float_scale;         // scale factor for float/double→int64 conversion (1.0 for integer types)
	bool clip_scale;            // true: scale outlier levels to nearest supported; false: omit them

	PacClipBindData(ClientContext &ctx, double mi_val, double correction_val, int clip_support,
	                double float_scale_val = 1.0, bool clip_scale_val = false)
	    : PacBindData(ctx, mi_val, correction_val, 1.0), clip_support_threshold(clip_support),
	      float_scale(float_scale_val), clip_scale(clip_scale_val) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<PacClipBindData>(*this);
		copy->total_update_count = 0;
		copy->suspicious_count = 0;
		copy->nonsuspicious_count = 0;
		return copy;
	}
	bool Equals(const FunctionData &other) const override {
		if (!PacBindData::Equals(other)) {
			return false;
		}
		auto *o = dynamic_cast<const PacClipBindData *>(&other);
		return o && clip_support_threshold == o->clip_support_threshold && float_scale == o->float_scale &&
		       clip_scale == o->clip_scale;
	}
};

// ============================================================================
// Shared bind functions for clip aggregates
// ============================================================================
static unique_ptr<FunctionData> PacClipBindWithScale(ClientContext &ctx, vector<unique_ptr<Expression>> &args,
                                                     const string &func_name, double float_scale = 1.0) {
	double mi = GetPacMiFromSetting(ctx);
	double correction = 1.0;
	if (2 < args.size()) {
		if (!args[2]->IsFoldable()) {
			throw InvalidInputException("%s: correction parameter must be a constant", func_name);
		}
		auto val = ExpressionExecutor::EvaluateScalar(ctx, *args[2]);
		correction = val.GetValue<double>();
		if (correction < 0.0) {
			throw InvalidInputException("%s: correction must be >= 0", func_name);
		}
	}
	int clip_support = 0;
	Value dc_val;
	if (ctx.TryGetCurrentSetting("pac_clip_support", dc_val) && !dc_val.IsNull()) {
		clip_support = static_cast<int>(dc_val.GetValue<int64_t>());
	}
	bool clip_scale_val = false;
	Value cs_val;
	if (ctx.TryGetCurrentSetting("pac_clip_scale", cs_val) && !cs_val.IsNull()) {
		clip_scale_val = cs_val.GetValue<bool>();
	}
	return make_uniq<PacClipBindData>(ctx, mi, correction, clip_support, float_scale, clip_scale_val);
}

static unique_ptr<FunctionData> PacClipBind(ClientContext &ctx, AggregateFunction &,
                                            vector<unique_ptr<Expression>> &args) {
	return PacClipBindWithScale(ctx, args, "pac_clip");
}

static unique_ptr<FunctionData> PacClipBindFloat(ClientContext &ctx, AggregateFunction &,
                                                 vector<unique_ptr<Expression>> &args) {
	return PacClipBindWithScale(ctx, args, "pac_clip", CLIP_FLOAT_SCALE);
}

static unique_ptr<FunctionData> PacClipBindDouble(ClientContext &ctx, AggregateFunction &,
                                                  vector<unique_ptr<Expression>> &args) {
	return PacClipBindWithScale(ctx, args, "pac_clip", CLIP_DOUBLE_SCALE);
}

} // namespace duckdb

#endif // PAC_CLIP_AGGR_HPP
