## Data Exploration & Quality Findings

จากการรัน query สำรวจข้อมูลใน `exploration.sql` กับ `vw_raw_customers`, `vw_raw_orders` และ `vw_exchange_rates` พบความผิดปกติ **6 กลุ่ม** สรุปได้ดังนี้

### Executive Summary

| ลำดับ (No.) | มุมมองข้อมูล (Dimension / Category) | ตารางและฟิลด์ที่พบปัญหา (Target) | ปัญหาที่ตรวจพบ (Anomaly Summary) | แนวทางจัดการใน Pipeline (Actionable Fix) |
|---|---|---|---|---|
| 1 | Uniqueness | `vw_raw_customers.customer_id` | พบ `customer_id` ซ้ำ (record เดียวกันถูกบันทึกมากกว่า 1 ครั้ง ด้วยข้อมูลคนละ version) | Deduplicate โดยเก็บ record ที่ `signup_date` ล่าสุดไว้เป็น record จริง |
| 2 | Completeness | `vw_raw_customers.email/phone`, `vw_raw_orders.order_date` | พบค่า `NULL` ในฟิลด์สำคัญ ทั้งฝั่งลูกค้า (email, phone) และฝั่ง order (order_date) | แทน email ว่างด้วย `unknown@domain.com` ส่วน phone ปล่อยว่างไว้เหมือนเดิม และออกแบบ logic ให้ pipeline ไม่ล้มเมื่อ `order_date` เป็น NULL |
| 3 | Data Consistency | `vw_raw_customers.phone` | รูปแบบเบอร์โทรไม่เป็นมาตรฐานเดียวกัน (มีวงเล็บ, ขีด, ตัวอักษรปน, รหัสประเทศ) | Standardize ด้วย regex ให้เหลือเฉพาะตัวเลขล้วนตามกฎที่โจทย์กำหนด |
| 4 | Validity | `vw_raw_orders.total_amount` | พบ `total_amount` ≤ 0 ซึ่งเป็น system error ไม่ใช่ transaction จริง | Filter ออกจาก `fct_orders` ทั้งหมด (ไม่นำเข้าฝั่ง analytics) |
| 5 | Referential Integrity | `vw_raw_orders.customer_id` | มี order อ้างถึง `customer_id` ที่ไม่มีอยู่จริงในตาราง customers (**Orphan Foreign Key**) | ใช้ **Unknown Dimension Pattern**: เพิ่ม dummy record เข้า `dim_customers` โดยตรง (`customer_id = -1, full_name = 'Unknown'`) แล้ว remap FK ของ order ที่ orphan ให้ชี้ไปที่ record นี้ใน `fct_orders` |
| 6 | Validity & Integrity | `vw_raw_orders` × `vw_exchange_rates` | สกุลเงินต่างประเทศบางรายการหา exchange rate ตรงวันที่ (`order_date`) ไม่เจอ เพราะช่วงวันที่ของ `vw_exchange_rates` สั้นกว่าช่วงวันที่ของ orders | ใช้ **LOCF (Last Observation Carried Forward)**: ใช้ rate ล่าสุดที่มีอยู่ก่อนหน้าวันที่ order แทนการปล่อยให้เป็น `NULL` |

---

### Detailed Breakdown

#### 1. Uniqueness — ข้อมูลลูกค้าซ้ำซ้อน
- **ปัญหาที่พบ**: มี `customer_id` เดียวกันปรากฏมากกว่า 1 record โดยแต่ละ record มีค่า `signup_date` และรายละเอียดการติดต่อ (email/phone) ต่างกัน ซึ่งเข้าใจได้ว่าเป็นการอัปเดตข้อมูลลูกค้าคนละช่วงเวลา
- **ความเสี่ยง/ผลกระทบ**: ถ้าไม่ deduplicate จะทำให้ `dim_customers` มี **Primary Key ซ้ำ** ส่งผลให้การ join กับ `fct_orders` นับจำนวนลูกค้าและคำนวณ CLV ผิดเพี้ยน (นับซ้ำ)
- **แนวทางแก้ไขในขั้นถัดไป**: Pipeline จะ group ตาม `customer_id` แล้วเลือกเฉพาะ record ที่มี `signup_date` ล่าสุดไว้เป็นตัวแทนก่อนโหลดเข้า `dim_customers`

#### 2. Completeness — ค่าที่ขาดหายไป
- **ปัญหาที่พบ**: `email`/`phone` เป็น `NULL` ในบางแถวของ customers และ `order_date` เป็น `NULL` ในบางแถวของ orders (เช่น order_id 117 ซึ่งมี `currency = USD`)
- **ความเสี่ยง/ผลกระทบ**: ค่า `NULL` ในฟิลด์ติดต่อทำให้ทีมอื่นใช้งานต่อยาก ส่วน `order_date` เป็น `NULL` กระทบโดยตรงต่อการ join หา exchange rate และการจัดกลุ่มข้อมูลตามช่วงเวลา
- **แนวทางแก้ไขในขั้นถัดไป**:
  - `email` ที่หายไป → แทนที่ด้วย `"unknown@domain.com"`
  - `phone` ที่หายไป → ปล่อยเป็น `NULL` หลัง standardize (ไม่ประดิษฐ์ค่าเบอร์โทรขึ้นมาเอง)
  - `order_date` เป็น `NULL` → **ไม่ตัดออก** จาก pipeline โดยใช้กติกา: ถ้า `currency = USD` ใช้ `total_amount` ตรงๆ ได้เลย (ไม่ต้องหา rate); ถ้า `currency ≠ USD` ให้ fallback เป็น USD เหมือนกฎ "หา exchange rate ไม่เจอ" (พร้อม log แจ้งเตือนไว้ debug) — และไม่กระทบ `customer_cohort` เพราะ cohort คำนวณจาก `signup_date` ของลูกค้า ไม่ใช่จาก `order_date`

#### 3. Data Consistency — รูปแบบเบอร์โทรไม่เป็นมาตรฐาน
- **ปัญหาที่พบ**: เบอร์โทรมีหลายรูปแบบปนกัน เช่น มีวงเล็บ/ขีดคั่น, มีรหัสประเทศนำหน้า, และบางค่าเป็นข้อความที่ไม่ใช่เบอร์จริง (มีตัวอักษรปนอยู่)
- **ความเสี่ยง/ผลกระทบ**: ทำให้ไม่สามารถใช้เบอร์โทรเป็น key เชื่อมกับระบบอื่น (เช่น CRM, marketing) ได้โดยตรง และเสี่ยงต่อการ export ข้อมูลผิดรูปแบบ
- **แนวทางแก้ไขในขั้นถัดไป**: เขียนฟังก์ชัน standardize เพื่อดึงเฉพาะตัวเลขออกมา ทดสอบแยกจาก DB ด้วย unit test ตามที่โจทย์กำหนดใน Part 3

#### 4. Validity & Integrity — ยอดเงิน, ลูกค้าที่ไม่มีอยู่จริง, และ exchange rate ไม่ครบ
- **ปัญหาที่พบ**:
  - **total_amount ≤ 0**: บาง order มียอดติดลบหรือเป็นศูนย์ ซึ่งเป็น system error
  - **Orphan customer_id**: order_id 106 และ 118 อ้างถึง `customer_id = 99` ที่ไม่มีอยู่จริงในตาราง customers
  - **Exchange rate date gap**: `vw_exchange_rates` มีข้อมูลถึงแค่ 2023-05-05 แต่ `vw_raw_orders` มีวันที่ยาวไปถึง 2023-05-14 ทำให้ order สกุลเงินต่างประเทศหลังวันที่ 05-05 หา rate ตรงวันไม่เจอ
- **ความเสี่ยง/ผลกระทบ**:
  - ยอดติดลบ/ศูนย์ทำให้ `lifetime_value_usd` คำนวณผิด
  - Orphan customer_id ถ้าตัดทิ้งจะทำให้ **รายได้รวม (Revenue) ถูกรายงานต่ำกว่าความจริง**
  - Exchange rate ไม่ครบวันจะทำให้ `usd_amount` เป็น `NULL` หากใช้ exact-date join ตรงๆ
- **แนวทางแก้ไขในขั้นถัดไป**:
  - **Filter order** ที่ `total_amount ≤ 0` ออกจาก `fct_orders` ทั้งหมด
  - **Unknown Dimension Pattern**: เพิ่ม dummy record (customer_id = -1, full_name = 'Unknown') เข้าไปใน dim_customers โดยตรง แล้ว remap customer_id ของ order ที่ orphan (106, 118) ใน fct_orders ให้ชี้ไปที่ -1 แทนการตัดทิ้งหรือปล่อยเป็น NULL 
  - **LOCF (Last Observation Carried Forward)**: เมื่อหา exchange rate ตรงวันที่ไม่เจอ ให้ใช้ rate ล่าสุดที่มีอยู่ก่อนหน้าวันที่ order นั้น (As-Of Join) แทนการปล่อยให้เป็น `NULL`