import argparse

def solve_arbitrage(odds_1: float, odds_2: float, bankroll: float) -> dict:
        """
        Optimal allocation if there is an arbitrage opportinity.
        """
        imp_1 = 1 / odds_1
        imp_2 = 1 / odds_2
        implied_sum = imp_1 + imp_2

        if implied_sum >= 1:
            return None

        stake_1 = bankroll * imp_1 / implied_sum
        stake_2 = bankroll * imp_2 / implied_sum
        profit  = stake_1 * odds_1 - bankroll

        return {
            "stake_1": round(stake_1, 2),
            "stake_2": round(stake_2, 2),
            "profit":  round(profit, 2),
            "roi_pct": round(profit / bankroll * 100, 4),
        }

def main() :
    result = solve_arbitrage(odds_1=2.10, odds_2=2.05, bankroll=1000) #Test
    print(result)

if __name__ =="__main__" :
    main()