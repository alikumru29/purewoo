import pyodbc
from config import MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD
from datetime import datetime

def get_mssql_connection():
    return pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}')

def save_order_to_mssql(order):
    with get_mssql_connection() as conn:
        cursor = conn.cursor()

        # Burada önceki kodunuzu kısaltarak yerleştirdim
        sip_tarih = "GETDATE()"
        sip_evrakno_seri = "PCW"
        current_date = datetime.now()
        formatted_date = current_date.strftime('%Y%m%d')
        formatted_date_without_time = current_date.strftime('%Y-%m-%d 00:00:00.000')
        print(formatted_date_without_time)
        
        # Sipariş serisine göre en son sıra numarasını bulalım
        cursor.execute(f"SELECT MAX(sip_evrakno_sira) FROM SIPARISLER WHERE sip_evrakno_seri='{sip_evrakno_seri}'")
        last_sira = cursor.fetchone()[0]
        if last_sira is None:
            last_sira = 0
        sip_evrakno_sira = last_sira + 1
        
        sip_satirno = 0
        sip_musteri_kod = "195.01.0002"
        
        for item in order['line_items']:
            sip_stok_kod = item['sku']
            
            # STOKLAR tablosundan sto_perakende_vergi bilgisini al
            cursor.execute(f"SELECT sto_perakende_vergi FROM STOKLAR WHERE sto_kod='{sip_stok_kod}'")
            vergi_kodu = cursor.fetchone()[0]
            if vergi_kodu == 7:
                vergi_orani = 10
            elif vergi_kodu == 8:
                vergi_orani = 20
            else:
                vergi_orani = 0
            
            sip_b_fiyat = float(item['price']) / (1 + vergi_orani/100)
            sip_miktar = item['quantity']
            sip_tutar = sip_b_fiyat * sip_miktar
            sip_vergi = (sip_b_fiyat * vergi_orani/100) * sip_miktar

            # SQL sorgusunu hazırla
            sql = f"""INSERT INTO SIPARISLER (
                sip_Guid,sip_DBCno,sip_SpecRECno,sip_iptal,sip_fileid,sip_hidden,sip_kilitli,sip_degisti,sip_checksum,sip_create_user,
                sip_create_date,sip_lastup_user,sip_lastup_date,sip_special1,sip_special2,sip_special3,sip_firmano,sip_subeno,sip_tarih,
                sip_teslim_tarih,sip_tip,sip_cins,sip_evrakno_seri,sip_evrakno_sira,sip_satirno,sip_belgeno,sip_belge_tarih,sip_satici_kod,
                sip_musteri_kod,sip_stok_kod,sip_b_fiyat,sip_miktar,sip_birim_pntr,sip_teslim_miktar,sip_tutar,sip_iskonto_1,sip_iskonto_2,
                sip_iskonto_3,sip_iskonto_4,sip_iskonto_5,sip_iskonto_6,sip_masraf_1,sip_masraf_2,sip_masraf_3,sip_masraf_4,sip_vergi_pntr,sip_vergi,
                sip_masvergi_pntr,sip_masvergi,sip_opno,sip_aciklama,sip_aciklama2,sip_depono,sip_OnaylayanKulNo,sip_vergisiz_fl,sip_kapat_fl,sip_promosyon_fl,
                sip_cari_sormerk,sip_stok_sormerk,sip_cari_grupno,sip_doviz_cinsi,sip_doviz_kuru,sip_alt_doviz_kuru,sip_adresno,sip_teslimturu,sip_cagrilabilir_fl,
                sip_iskonto1,sip_iskonto2,sip_iskonto3,sip_iskonto4,sip_iskonto5,sip_iskonto6,sip_masraf1,sip_masraf2,sip_masraf3,sip_masraf4,sip_isk1,
                sip_isk2,sip_isk3,sip_isk4,sip_isk5,sip_isk6,sip_mas1,sip_mas2,sip_mas3,sip_mas4,sip_Exp_Imp_Kodu,sip_kar_orani,sip_durumu,sip_planlananmiktar,
                sip_parti_kodu,sip_lot_no,sip_projekodu,sip_fiyat_liste_no,sip_Otv_Pntr,sip_Otv_Vergi,sip_otvtutari,sip_OtvVergisiz_Fl,sip_paket_kod,
                sip_harekettipi,sip_kapatmanedenkod,sip_gecerlilik_tarihi,sip_onodeme_evrak_tip,sip_onodeme_evrak_seri,sip_onodeme_evrak_sira,
                sip_rezervasyon_miktari,sip_rezerveden_teslim_edilen,sip_HareketGrupKodu1,sip_HareketGrupKodu2,sip_HareketGrupKodu3,sip_Olcu1,sip_Olcu2,sip_Olcu3,sip_Olcu4,
                sip_Olcu5,sip_FormulMiktarNo,sip_FormulMiktar,sip_satis_fiyat_doviz_cinsi,sip_satis_fiyat_doviz_kuru,sip_eticaret_kanal_kodu,sip_Tevkifat_turu,
                sip_otv_tevkifat_turu,sip_otv_tevkifat_tutari
                ) 
                VALUES (
            NEWID(),0,0,0,21,0,0,0,0,6,'{sip_tarih}',1,'{sip_tarih}',N'',N'',N'',0,0,'{formatted_date_without_time}','{formatted_date_without_time}',0,0,'{sip_evrakno_seri}',{sip_evrakno_sira},{sip_satirno},
            N'','{formatted_date_without_time}',N'','{sip_musteri_kod}','{sip_stok_kod}',{sip_b_fiyat},{sip_miktar},1,0,{sip_tutar},0,0,0,0,0,0,0,0,0,0,8,{sip_vergi},0,0,0,N'',N'',1,0,0,0,0,N'10',N'10',0,0,1.000000000000,
            27.526400000000,1,N'01',1,0,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,N'',0,0,0,N'',0,N'',1,0,0,0,0,N'',0,N'','1899-12-30 00:00:00.000',0,N'',0,0,0,N'',N'',N'',0,0,0,0,0,0,0,0,1.000000000000,N'',0,0,0
            )"""

            # Sorguyu çalıştır
            print(sql)
            cursor.execute(sql)
            sip_satirno += 1  # Bir sonraki ürün için satır numarasını artır
        conn.commit()
