fts_separated=["1.", "2.", "3."]
fts = []


num_fts = int(math.pow(2, len(fts_separated)))
fts_sel = []
for i in range(0, num_fts):
  fts_sel.append([])
for i in range(0, len(fts_separated)):
  rnd = int(math.pow(2, i))
  pattern = [True] * rnd + [False] * rnd
  pattern *= int(num_fts / 2 / rnd)
  for j in range(0, num_fts):
    fts_sel[j].append(pattern[j])
for sel in fts_sel:
  ft = ""
  for i in range (0, len(sel)):
    if sel[i]:
      ft += fltr[i]
  fts.append(ft)
