# MLB Kalshi Strategy v3: ask_heavy DCA + Concurrent Kelly

## One-Liner

Kalshi MLB game-winner 시장에서 매도 압력 과잉(ask_heavy)을 감지하고, 지속 기간 동안 초당 $1씩 DCA 진입, concurrent Kelly로 게임당 cap을 동적 조정, settlement까지 보유.

---

## Edge Source

Kalshi MLB 오더북의 구조적 비효율. 모델 기반 아님.

- **ask_heavy**: depth_ratio < 0.4 → observation-level PnL +0.1079 (유의)
- **홈 바이어스 독립**: away_favored에서도 유의
- **Adverse selection 없음** (60초 기준)
- **DCA**: 첫 진입 대비 평균 5.92¢ 가격 개선

---

## Entry

| Parameter | Value |
|---|---|
| Trigger | depth_ratio < 0.4 |
| Order type | YES limit at mid (maker fee $0) |
| 진입 방식 | ask_heavy 활성 동안 1초 간격 $1 반복 진입 (DCA) |
| ask_heavy 해제 시 | 신규 주문 중단, 포지션 유지 |
| ask_heavy 재활성 시 | cap 미도달이면 DCA 재개 |

---

## Exit

Settlement까지 보유. 예외 없음.

---

## Sizing: Concurrent Kelly

```
game_cap = bankroll × kelly_fraction × f* / n_concurrent_games
```

| Parameter | Value |
|---|---|
| Kelly fraction | 1/4 (0.25) |
| f* (estimated) | 0.535 (μ=0.0926, σ²=0.1731) |
| Effective single-game % | 13.4% of bankroll |
| With 8 concurrent games | ~1.7% per game |
| Max total exposure | 40% of bankroll |
| Cap floor | $50/game |
| Cap ceiling | 20% of bankroll per game |

**동적 조정**: 새 게임 시작 시 현재 concurrent 수로 나눔. 진행 중 게임의 cap은 변경 안 함. 게임 종료 시 capital 해제 → 다음 게임부터 반영.

---

## Backtest Results (98 games)

| Version | PnL/$1 | Sharpe | Significant | Max DD | ROI |
|---|---|---|---|---|---|
| v1 (first entry) | +0.033 | 0.071 | NO | -$4.20/unit | — |
| v2 (DCA $100) | +0.043 | 0.093 | NO | -$390 | — |
| v3 (DCA $1000) | +0.093 | 0.222 | YES | -$3,321 | +14.5% |
| v4 (Seq. Kelly 1/8) | — | 0.212 | YES | -$7,305 | +76.6% |
| **v5 (Conc. Kelly 1/4)** | — | **0.258** | **YES** | **-$2,604** | **+45.1%** |

**v5 Concurrent Kelly 1/4 ($25K → $36,283):**
- 89 trades, mean cap $6,150/game
- Max DD -9.1% of bankroll ($2,604)
- Worst single game: -$2,765

---

## Expected Performance

| Metric | Value |
|---|---|
| Annual ROI (backtest) | +45.1% |
| Annual ROI (conservative, ÷2) | ~22% |
| Max drawdown | 9.1% of bankroll |
| Win rate | ~60% |
| Mean concurrent games | 8.6 |

---

## Deployment

| Phase | 기간 | 방식 | Gate |
|---|---|---|---|
| Phase 0 | 2주 | 로깅 + paper trade | fill rate >70%, depth분포 일치 |
| Phase 1 | 50게임 | live, 1/4 conc. Kelly | PnL >0, no adverse selection |
| Phase 2 | 100게임+ | live, 결과 기반 조정 | Sharpe >0.1 유지 |

### Kill Conditions
- 30게임 연속 negative cumulative PnL
- Live fill rate <50%
- Adverse selection 관찰 (50+게임)
- Kalshi maker fee 도입