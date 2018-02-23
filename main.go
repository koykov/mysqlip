package main

import (
	"io"
	"bufio"
	"os"
	"os/exec"
	"strings"
	"strconv"
	"flag"
	"fmt"
	"time"
	"log"

	"github.com/google/gxui/math"
	tui "github.com/gizak/termui"
)

func main() {
	var (
		percent float64
		prev_value float64 = -1.0

		qty_table int = 0
		qty_ins int = 0
		qty_other int = 0
		qty_total int = 0
		avoid_calc bool = false

		interrupt bool = false
	)

	// Parse CLI options.
	hostPtr := flag.String("h", "", "Mysql host.")
	userPtr := flag.String("u", "", "Mysql username.")
	passPtr := flag.String("p", "", "Mysql password.")
	forcePtr := flag.Bool("f", false, "Continue even if we get an SQL error..")
	flag.Parse()

	// Parse CLI args.
	cli_args_raw := os.Args
	cli_args := []string{}
	// - exclude flags from args.
	var ignore_pos = 0;
	for key, arg := range cli_args_raw {
		if key <= ignore_pos {
			continue
		}
		if arg == "-h" || arg == "-u" || arg == "-p" {
			ignore_pos = key + 1
			continue
		}
		if arg == "-f" {
			continue
		}
		cli_args = append(cli_args, arg)
		//fmt.Printf("%d - %#v\n", key, arg)
	}
	db := ""
	if len(cli_args) >= 1 {
		db = cli_args[0]
	} else {
		log.Fatal("Unknown targed database.")
	}

	// Check stdin.
	stats, err := os.Stdin.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// Exec mysql process
	args := []string{}
	if len(*hostPtr) > 0 {
		args = append(args, "-h")
		args = append(args, *hostPtr)
	}
	if len(*userPtr) > 0 {
		args = append(args, "-u")
		args = append(args, *userPtr)
	}
	if len(*passPtr) > 0 {
		args = append(args, "-p" + *passPtr)
	}
	if *forcePtr {
		args = append(args, "-f")
	}
	args = append(args, db)

	mysqlp := exec.Command("mysql", args...)
	mysql_stdin, err := mysqlp.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	defer mysql_stdin.Close() // mysql proc will close when work will finished.

	// Redirect stdout/stderr to mysql process.
	mysqlp.Stdout = os.Stdout
	mysqlp.Stderr = os.Stderr
	if err = mysqlp.Start(); err != nil {
		log.Fatal(err)
	}

	// Prepare TUI.
	err = tui.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer tui.Close()

	// Progress indicator.
	progress_gauge := tui.NewGauge()
	progress_gauge.Percent = 0
	progress_gauge.Width = tui.TermWidth()
	progress_gauge.Height = 3
	progress_gauge.BorderLabel = "progress"
	progress_gauge.BarColor = tui.ColorGreen
	progress_gauge.BorderFg = tui.ColorWhite
	progress_gauge.BorderLabelFg = tui.ColorWhite

	// Statistics table.
	stats_rows := [][]string{
		[]string{"tables", "inserts", "others", "total", "spent", "eta"},
		[]string{"0", "0", "0", "0", "0", "0", "?"},
	}
	stats_table := tui.NewTable()
	stats_table.Rows = stats_rows
	stats_table.BorderLabel = "stats"
	stats_table.BorderLabelFg = tui.ColorWhite
	stats_table.FgColor = tui.ColorWhite
	stats_table.BgColor = tui.ColorDefault
	stats_table.Y = 0
	stats_table.X = 0
	stats_table.Width = tui.TermWidth()
	stats_table.Height = 5
	tui.Render(stats_table)

	// TUI layout.
	tui.Body.AddRows(
		tui.NewRow(
			tui.NewCol(12, 0, stats_table),
		),
		tui.NewRow(
			tui.NewCol(12, 0, progress_gauge),
		),
	)
	tui.Body.Align()

	// TUI handlers.
	tui.Handle("/sys/kbd/q", func(tui.Event) {
		tui.StopLoop()
		interrupt = true
	})

	tui.Handle("/sys/wnd/resize", func(e tui.Event) {
		tui.Body.Width = tui.TermWidth()
		tui.Body.Align()
		tui.Clear()
		tui.Render(tui.Body)
	})

	// Import loop.
	total := int(stats.Size())
	reader := bufio.NewReader(os.Stdin)
	line := ""
	err = io.EOF
	size := 0
	tt_start := time.Now()
	tt_spent := time.Since(tt_start)
	tt_eta := 0
	for true {
		if interrupt {
			break
		}

		line, err = reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		io.WriteString(mysql_stdin, line) // Send line to mysql's stdin.

		// Collect statistics.
		if !avoid_calc {
			if strings.Contains(line, "CREATE TABLE ") {
				qty_table++
				avoid_calc = true
			} else if strings.Contains(line, "INSERT INTO ") {
				qty_ins++
				//} else if strings.Contains(line, "DROP TABLE") || strings.Contains(line, "") {
			} else if len(line) > 1 && !strings.Contains(line, "--") {
				qty_other++
			}
		} else if strings.Contains(line, ") ENGINE") {
			avoid_calc = false
		}
		qty_total = qty_table + qty_ins + qty_other

		// Calculate progress.
		tt_spent = time.Since(tt_start)
		size += len(line)
		percent = float64(size) / float64(total) * 100

		// Display progress.
		if (int(prev_value) != int(percent)) {
			// Calculate ETA.
			if percent > 0 && math.Round(float32(percent)) % 5 == 0 {
				tt_eta = int(tt_spent.Seconds() * 100 / percent);
			}

			// Update UI.
			stats_rows = [][]string{
				[]string{"tables", "inserts", "others", "total", "spent", "eta"},
				[]string{
					strconv.Itoa(qty_table),
					strconv.Itoa(qty_ins),
					strconv.Itoa(qty_other),
					strconv.Itoa(qty_total),
					fmt.Sprintf("%d:%d:%d", math.Round(float32(tt_spent.Hours())), math.Round(float32(tt_spent.Minutes())), math.Round(float32(tt_spent.Seconds()))),
					fmt.Sprintf("%d:%d:%d", math.Round(float32(tt_eta / 3600)), math.Round(float32(tt_eta % 3600 / 60)), math.Round(float32((tt_eta % 3600) % 60))),
				},
			}
			stats_table.Rows = stats_rows
			tui.Render(stats_table)

			progress_gauge.Percent = int(percent)
			tui.Render(progress_gauge)

			prev_value = percent
		}
	}

	// Final update.
	stats_rows = [][]string{
		[]string{"tables", "inserts", "others", "total", "spent", "eta"},
		[]string{
			strconv.Itoa(qty_table),
			strconv.Itoa(qty_ins),
			strconv.Itoa(qty_other),
			strconv.Itoa(qty_total),
			fmt.Sprintf("%d:%d:%d", math.Round(float32(tt_spent.Hours())), math.Round(float32(tt_spent.Minutes())), math.Round(float32(tt_spent.Seconds()))),
			"00:00:00",
		},
	}
	stats_table.Rows = stats_rows
	tui.Render(stats_table)

	progress_gauge.BorderLabel = "done, press \"q\" to exit"
	progress_gauge.Percent = 100
	tui.Render(progress_gauge)

	tui.Loop()
}
