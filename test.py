fe = FeatureExtractor(df_trans_all)

fe.add_filter(["company=104460040", "brand=7668", "category=1726"])
fe.add_feature("purchaseamount", functions=["exist"])
f_df = fe.extract()

