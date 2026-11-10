import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt
from search_engine_v2 import MediaPersonnelEngine

console = Console()
engine = MediaPersonnelEngine()

def run_org_dept_search():
    console.print("\n[bold cyan]1. [언론사 조직] 매체·부서 검색[/bold cyan]")
    console.print("[dim]※ 출처: 연합뉴스·뉴스1 [인사]/<인사> (최근 40년 전수 데이터)[/dim]")
    
    company = Prompt.ask("• 언론사명 (필수)", default="조선일보")
    dept = Prompt.ask("• 부서명 (선택, 엔터 시 전체)", default="")
    pos = Prompt.ask("• 직급 (선택, 엔터 시 전체)", default="")

    results = engine.search_by_org_and_dept(company=company, department=dept, position=pos)

    if not results:
        console.print(f"[yellow]'{company}'의 조건에 부합하는 언론사 인사 기사 기록이 없습니다.[/yellow]")
        return

    table = Table(
        title=f"검색 결과: {company} | 부서: {dept or '전체'} | 직급: {pos or '전체'} ({len(results)}건)",
        show_header=True,
        header_style="bold magenta"
    )
    table.add_column("기자명", style="bold green", width=10)
    table.add_column("소속 언론사", width=15)
    table.add_column("발령 부서", width=18)
    table.add_column("직급", width=12)
    table.add_column("구분", justify="center", width=8)
    table.add_column("발령일", justify="center", width=12)
    table.add_column("출처 기사", width=38)

    for r in results:
        table.add_row(
            r["name"],
            r["company"],
            r["department"],
            r["position"],
            r["action"],
            r["event_date"],
            r["source"][:35] + "..."
        )

    console.print(table)


def run_media_name_search():
    console.print("\n[bold cyan]2. [기자 이력] 매체·이름 검색[/bold cyan]")
    console.print("[dim]※ 출처: 연합뉴스·뉴스1 [인사]/<인사> (최근 40년 전수 데이터)[/dim]")
    
    company = Prompt.ask("• 대상 언론사명 (필수)", default="한국경제")
    name = Prompt.ask("• 기자명 (필수)")

    res = engine.search_by_media_and_name(company=company, name=name)

    if not res["found"]:
        console.print(f"[yellow]{res['message']}[/yellow]")
        return

    console.print(Panel(
        f"[bold white]{res['name']}[/bold white] 기자 - [bold yellow]{res['company']}[/bold yellow] 인사 발령 기록 (최근 40년: 총 {res['total_records']}건)",
        title="언론사 인사 프로필",
        expand=False
    ))

    # 발령 타임라인
    t_table = Table(title="[발령 타임라인]", show_header=True, header_style="bold blue")
    t_table.add_column("발령일자", justify="center", width=12)
    t_table.add_column("부서", width=18)
    t_table.add_column("직급", width=12)
    t_table.add_column("발령구분", justify="center", width=10)
    t_table.add_column("출처 기사", width=42)

    for item in res["timeline"]:
        t_table.add_row(item["date"], item["department"], item["position"], item["action"], item["source"])
    console.print(t_table)

    # 6개월 윈도우 기반 이직 내역
    if res["turnover_history"]:
        to_table = Table(title="[언론사 간 이직(유입/유출) 감지 내역]", show_header=True, header_style="bold red")
        to_table.add_column("이직 기준일", justify="center", width=12)
        to_table.add_column("이전 소속 (부서)", width=22)
        to_table.add_column("이직 소속 (부서)", width=22)
        to_table.add_column("공백 일수", justify="right", width=10)

        for t in res["turnover_history"]:
            to_table.add_row(
                t["turnover_date"],
                f"{t['from_company']} ({t['from_dept'] or '-'})",
                f"{t['to_company']} ({t['to_dept'] or '-'})",
                f"{t['gap_days']}일"
            )
        console.print(to_table)
    else:
        console.print("[dim]※ 최근 40년 내 타 언론사 간 이직 조건(6개월 내 이동)에 부합하는 이벤트가 없습니다.[/dim]")


def main():
    while True:
        console.print("\n[bold green]=== 언론사 인사 트래커 (연합뉴스·뉴스1 / 40년 데이터) ===[/bold green]")
        console.print("1. 매체·부서 검색 (직급 필터 포함)")
        console.print("2. 매체·이름 검색 (타임라인 + 이직 추적)")
        console.print("0. 종료")

        choice = Prompt.ask("메뉴 번호 선택", choices=["1", "2", "0"], default="1")

        if choice == "1":
            run_org_dept_search()
        elif choice == "2":
            run_media_name_search()
        elif choice == "0":
            console.print("[bold]프로그램을 종료합니다.[/bold]")
            sys.exit(0)

if __name__ == "__main__":
    main()
