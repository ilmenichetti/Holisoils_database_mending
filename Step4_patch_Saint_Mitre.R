
# LEtter from Charlotte Byriol
# Hi everyone,
# 
# Thank you for your patience.
# 
# After discussing with Solène and Mathieu (who are handling the St Christol data), it seems that there is a problem with the files sent and I think you should check with the other partners. 
# Contrary to the information given in the "Co2_ranges_by_site" file, there is no difference in surface area between Saint Mitre, St Christol, and Gamiz. 
# The surface area is 0.7854 dm2 (I had entered a different unit for Saint Mitre in the Holisoils plateform). 
# As far as volume is concerned (1,8096 L), there is no problem for Saint Mitre (the data is based on an average, unlike Saint Christol, where each PVC tube was measured). 
# Could you please correct this? The values should be more consistent. I look forward to see the comparison between the sites. 
# 
# I remain available for any further information. 
# 
# Have a nice summer,


# Calculation of flux (from Jani)
# f =  (V / A) * M * 3600 * (101325 * slope * 1e-6) / (8.31446 * (T + T_0)) ,
# group everything except A
# f = (V × M × 3600 × (101325 × slope × 1e-6) / (8.31446 × (T + T_0))) / A
# group all numerator in a constant K
# K = (V × M × 3600 × (101325 × slope × 1e-6) / (8.31446 × (T + T_0))) 
# Formula becomes
# f = K / A
#
#f_old = K / A_old
#f_new = K / A_new
#
#f_new = f_old × (A_old / A_new)

saint_mitre <- read.csv("./Holisoils_GHG_data/Saint Mitre/Saint Mitrewhole.csv")

A_old = saint_mitre$area

A_new = 0.7854

f_old = saint_mitre$autotrim_flux

f_new = f_old * (A_old/A_new)
res_new = saint_mitre$autotrim_resid * (A_old/A_new)

saint_mitre$autotrim_flux <- f_new
saint_mitre$autotrim_resid <- res_new


write.csv(saint_mitre, "./Holisoils_GHG_data/Saint Mitre/Saint Mitrewhole.csv", row.names = FALSE)
