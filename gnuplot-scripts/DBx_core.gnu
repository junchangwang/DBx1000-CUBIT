reset
set size 0.99,1.0
#set term pdf font ",10" size 5.2, 2.4
set ylabel "Throughput (queries/s)" offset -0.8,0,0 font ',25'
set xlabel "Number of worker threads" font ',29'
set xtics ("1" 1,"4" 4, "8" 8, "16" 16, "24" 24, "32" 32)
set yrange [0:]
set xrange [0:35]
set terminal png font ',15'
set output "../graphs_DBx/core/core.png"
set key font ",20" reverse top left vertical Left
set xtics  font ",29"
# set ytics ("0" 0,"10" 10,"20" 20,"30" 30) font ",29"

plot "../dat_DBx/core.dat" every 6::5::41 title "CUBIT" lc rgb "blue" lw 8 ps 1.5 pt 6 with linespoints,\
      "../dat_DBx/core.dat" every 6::3::39 title "Bw-Tree" lc rgb "brown" lw 8 ps 1.5 pt 8 dt "-" with linespoints,\
      "../dat_DBx/core.dat" every 6::4::40 title "ART" lc rgb "dark-violet" lw 8 ps 1.5 pt 2 dt "-" with linespoints,\
      "../dat_DBx/core.dat" every 6::2::38 title "B^+-Tree" lc rgb "dark-orange" lw 8 ps 1.5 pt 4 dt "-" with linespoints,\
      "../dat_DBx/core.dat" every 6::1::37 title "Hash" lc rgb "sea-green" lw 8 ps 1.5 pt 10 dt 9 with linespoints,\
      "../dat_DBx/core.dat" every 6::0::36 title "Scan" lc rgb "black" lw 8 ps 1.5 pt 12 dt 5 with linespoints

set terminal eps size 5, 3 font 'Linux Libertine O,22'  enhanced
set output "../graphs_DBx/core/core.eps"
replot

############################## histograms index and tuples ###############################
reset
set key font ",25" reverse invert top left Left

set terminal png font ',15'
set output "../graphs_DBx/core/histograms.png"

set origin 0,0.05
set size 1,0.98

set yrange [0:1.6]
#set autoscale y

#set title "Breakdown of Updates"
set style data histograms
set style histogram rowstacked
set boxwidth 0.75 relative
set style fill solid 1.0 border -1
set xtics nomirror rotate by -45 scale 0

set ylabel "Latency (s)" offset 0.1,0,0 font ',29'

set label "1" font ",29" at screen 0.3, screen 0.04
set label "8" font ",29" at screen 0.50, screen 0.04
set label "32 (cores)" font ",29" at screen 0.74, screen 0.04

set xtics font ",21"
set ytics font ",21"

plot "../dat_DBx/index_time.dat" using ($3/1e6):xtic(4) title "Fetching tuples" with histograms fill pattern 7 border rgb "black" lc rgb "black", \
     "../dat_DBx/index_time.dat" using ($2/1e6):xtic(4) title "Reading index" with histograms fs solid border rgb "black" lc rgb "black"
     

set terminal eps size 5, 3 font 'Linux Libertine O,21'  enhanced
set output "../graphs_DBx/core/histograms.eps"
replot

############################## histograms run time ###############################
# reset
# set key font ",20" reverse invert top left Left
# 
# set terminal png font ',15'
# set output "../graphs_DBx/core/histograms_run_time.png"
# 
# set origin 0,0.05
# set size 1,0.95
# 
# #set yrange [0:3e3]
# # set autoscale y
# set boxwidth 0.8
# set style fill solid
# # set boxwidth 0.75 relative
# # set style fill solid 1.0 border -1
# # set xtics nomirror rotate by -45 scale 0
# set ylabel "Latency (ms)" offset 1,0,0 font ',28'
# set xrange [-0.5:28]
# unset xtics
# set label "1" font ",25" at screen 0.19, screen 0.04
# set label "2" font ",25" at screen 0.3, screen 0.04
# set label "4" font ",25" at screen 0.415, screen 0.04
# set label "8" font ",25" at screen 0.535, screen 0.04
# set label "16" font ",25" at screen 0.64, screen 0.04
# set label "24" font ",25" at screen 0.75, screen 0.04
# set label "32(cores)" font ",25" at screen 0.83, screen 0.04
# 
# # set xticsfont ",8"
# 
# # plot  "../dat_DBx/run_time.dat" every 3   using 1:($2/1e3) title "Hash" with histograms fs solid border rgb "black" lc rgb "black"
# 
# plot "../dat_DBx/run_time.dat" every 3    using 1:($2/1e3) title "Hash" with boxes fs solid 1.0 border rgb "black" lc rgb "white", \
#      "../dat_DBx/run_time.dat" every 3::1 using 1:($2/1e3) title "B^+-Tree" with boxes fs solid 1.0 border rgb "black" lc rgb "grey", \
#      "../dat_DBx/run_time.dat" every 3::2 using 1:($2/1e3) title "CUBIT" with boxes fs solid 1.0 border rgb "black" lc rgb "black"     
# 
# set terminal eps size 5, 3 font 'Linux Libertine O,20'  enhanced
# set output "../graphs_DBx/core/histograms_run_time.eps"
# replot
# 
