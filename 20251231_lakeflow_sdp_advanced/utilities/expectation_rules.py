def get_master_check_rules():
  """
  販売履歴に対してマスタ（商品・顧客）が正しく紐付いているかを確認するルールを定義します。
  紐付けに失敗し、IDがNULLになったレコードを検出するために使用します。
  """
  return {
      # 販売履歴に商品マスターを左外部結合した時、product_idがNULLである場合
      # （商品マスターに存在しない商品の販売履歴）は、不正データとみなす
      "product_id_is_not_null": "product_id IS NOT NULL",
      
      # 販売履歴に顧客マスターを左外部結合した時、user_idがNULLである場合
      # （顧客マスターに存在しない加入者の販売履歴）は、不正データとみなす
      # ※今回の「伊藤さん(U04)」がここに引っかかり、除外(DROP)される伏線になります
      "user_id_is_not_null": "user_id IS NOT NULL",
  }