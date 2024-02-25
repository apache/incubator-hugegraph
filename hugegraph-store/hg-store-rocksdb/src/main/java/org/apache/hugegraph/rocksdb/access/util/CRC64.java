/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.rocksdb.access.util;

import java.util.zip.Checksum;

public class CRC64 implements Checksum {
    private static final long[] CRC_TABLE = new long[]{0x0000000000000000L, 0x42F0E1EBA9EA3693L,
                                                       0x85E1C3D753D46D26L, 0xC711223CFA3E5BB5L,
                                                       0x493366450E42ECDFL, 0x0BC387AEA7A8DA4CL,
                                                       0xCCD2A5925D9681F9L, 0x8E224479F47CB76AL,
                                                       0x9266CC8A1C85D9BEL, 0xD0962D61B56FEF2DL,
                                                       0x17870F5D4F51B498L, 0x5577EEB6E6BB820BL,
                                                       0xDB55AACF12C73561L, 0x99A54B24BB2D03F2L,
                                                       0x5EB4691841135847L, 0x1C4488F3E8F96ED4L,
                                                       0x663D78FF90E185EFL, 0x24CD9914390BB37CL,
                                                       0xE3DCBB28C335E8C9L, 0xA12C5AC36ADFDE5AL,
                                                       0x2F0E1EBA9EA36930L, 0x6DFEFF5137495FA3L,
                                                       0xAAEFDD6DCD770416L, 0xE81F3C86649D3285L,
                                                       0xF45BB4758C645C51L, 0xB6AB559E258E6AC2L,
                                                       0x71BA77A2DFB03177L, 0x334A9649765A07E4L,
                                                       0xBD68D2308226B08EL, 0xFF9833DB2BCC861DL,
                                                       0x388911E7D1F2DDA8L, 0x7A79F00C7818EB3BL,
                                                       0xCC7AF1FF21C30BDEL, 0x8E8A101488293D4DL,
                                                       0x499B3228721766F8L, 0x0B6BD3C3DBFD506BL,
                                                       0x854997BA2F81E701L, 0xC7B97651866BD192L,
                                                       0x00A8546D7C558A27L, 0x4258B586D5BFBCB4L,
                                                       0x5E1C3D753D46D260L, 0x1CECDC9E94ACE4F3L,
                                                       0xDBFDFEA26E92BF46L, 0x990D1F49C77889D5L,
                                                       0x172F5B3033043EBFL, 0x55DFBADB9AEE082CL,
                                                       0x92CE98E760D05399L, 0xD03E790CC93A650AL,
                                                       0xAA478900B1228E31L, 0xE8B768EB18C8B8A2L,
                                                       0x2FA64AD7E2F6E317L, 0x6D56AB3C4B1CD584L,
                                                       0xE374EF45BF6062EEL, 0xA1840EAE168A547DL,
                                                       0x66952C92ECB40FC8L, 0x2465CD79455E395BL,
                                                       0x3821458AADA7578FL, 0x7AD1A461044D611CL,
                                                       0xBDC0865DFE733AA9L, 0xFF3067B657990C3AL,
                                                       0x711223CFA3E5BB50L, 0x33E2C2240A0F8DC3L,
                                                       0xF4F3E018F031D676L, 0xB60301F359DBE0E5L,
                                                       0xDA050215EA6C212FL, 0x98F5E3FE438617BCL,
                                                       0x5FE4C1C2B9B84C09L, 0x1D14202910527A9AL,
                                                       0x93366450E42ECDF0L, 0xD1C685BB4DC4FB63L,
                                                       0x16D7A787B7FAA0D6L, 0x5427466C1E109645L,
                                                       0x4863CE9FF6E9F891L, 0x0A932F745F03CE02L,
                                                       0xCD820D48A53D95B7L, 0x8F72ECA30CD7A324L,
                                                       0x0150A8DAF8AB144EL, 0x43A04931514122DDL,
                                                       0x84B16B0DAB7F7968L, 0xC6418AE602954FFBL,
                                                       0xBC387AEA7A8DA4C0L, 0xFEC89B01D3679253L,
                                                       0x39D9B93D2959C9E6L, 0x7B2958D680B3FF75L,
                                                       0xF50B1CAF74CF481FL, 0xB7FBFD44DD257E8CL,
                                                       0x70EADF78271B2539L, 0x321A3E938EF113AAL,
                                                       0x2E5EB66066087D7EL, 0x6CAE578BCFE24BEDL,
                                                       0xABBF75B735DC1058L, 0xE94F945C9C3626CBL,
                                                       0x676DD025684A91A1L, 0x259D31CEC1A0A732L,
                                                       0xE28C13F23B9EFC87L, 0xA07CF2199274CA14L,
                                                       0x167FF3EACBAF2AF1L, 0x548F120162451C62L,
                                                       0x939E303D987B47D7L, 0xD16ED1D631917144L,
                                                       0x5F4C95AFC5EDC62EL, 0x1DBC74446C07F0BDL,
                                                       0xDAAD56789639AB08L, 0x985DB7933FD39D9BL,
                                                       0x84193F60D72AF34FL, 0xC6E9DE8B7EC0C5DCL,
                                                       0x01F8FCB784FE9E69L, 0x43081D5C2D14A8FAL,
                                                       0xCD2A5925D9681F90L, 0x8FDAB8CE70822903L,
                                                       0x48CB9AF28ABC72B6L, 0x0A3B7B1923564425L,
                                                       0x70428B155B4EAF1EL, 0x32B26AFEF2A4998DL,
                                                       0xF5A348C2089AC238L, 0xB753A929A170F4ABL,
                                                       0x3971ED50550C43C1L, 0x7B810CBBFCE67552L,
                                                       0xBC902E8706D82EE7L, 0xFE60CF6CAF321874L,
                                                       0xE224479F47CB76A0L, 0xA0D4A674EE214033L,
                                                       0x67C58448141F1B86L, 0x253565A3BDF52D15L,
                                                       0xAB1721DA49899A7FL, 0xE9E7C031E063ACECL,
                                                       0x2EF6E20D1A5DF759L, 0x6C0603E6B3B7C1CAL,
                                                       0xF6FAE5C07D3274CDL, 0xB40A042BD4D8425EL,
                                                       0x731B26172EE619EBL, 0x31EBC7FC870C2F78L,
                                                       0xBFC9838573709812L, 0xFD39626EDA9AAE81L,
                                                       0x3A28405220A4F534L, 0x78D8A1B9894EC3A7L,
                                                       0x649C294A61B7AD73L, 0x266CC8A1C85D9BE0L,
                                                       0xE17DEA9D3263C055L, 0xA38D0B769B89F6C6L,
                                                       0x2DAF4F0F6FF541ACL, 0x6F5FAEE4C61F773FL,
                                                       0xA84E8CD83C212C8AL, 0xEABE6D3395CB1A19L,
                                                       0x90C79D3FEDD3F122L, 0xD2377CD44439C7B1L,
                                                       0x15265EE8BE079C04L, 0x57D6BF0317EDAA97L,
                                                       0xD9F4FB7AE3911DFDL, 0x9B041A914A7B2B6EL,
                                                       0x5C1538ADB04570DBL, 0x1EE5D94619AF4648L,
                                                       0x02A151B5F156289CL, 0x4051B05E58BC1E0FL,
                                                       0x87409262A28245BAL, 0xC5B073890B687329L,
                                                       0x4B9237F0FF14C443L, 0x0962D61B56FEF2D0L,
                                                       0xCE73F427ACC0A965L, 0x8C8315CC052A9FF6L,
                                                       0x3A80143F5CF17F13L, 0x7870F5D4F51B4980L,
                                                       0xBF61D7E80F251235L, 0xFD913603A6CF24A6L,
                                                       0x73B3727A52B393CCL, 0x31439391FB59A55FL,
                                                       0xF652B1AD0167FEEAL, 0xB4A25046A88DC879L,
                                                       0xA8E6D8B54074A6ADL, 0xEA16395EE99E903EL,
                                                       0x2D071B6213A0CB8BL, 0x6FF7FA89BA4AFD18L,
                                                       0xE1D5BEF04E364A72L, 0xA3255F1BE7DC7CE1L,
                                                       0x64347D271DE22754L, 0x26C49CCCB40811C7L,
                                                       0x5CBD6CC0CC10FAFCL, 0x1E4D8D2B65FACC6FL,
                                                       0xD95CAF179FC497DAL, 0x9BAC4EFC362EA149L,
                                                       0x158E0A85C2521623L, 0x577EEB6E6BB820B0L,
                                                       0x906FC95291867B05L, 0xD29F28B9386C4D96L,
                                                       0xCEDBA04AD0952342L, 0x8C2B41A1797F15D1L,
                                                       0x4B3A639D83414E64L, 0x09CA82762AAB78F7L,
                                                       0x87E8C60FDED7CF9DL, 0xC51827E4773DF90EL,
                                                       0x020905D88D03A2BBL, 0x40F9E43324E99428L,
                                                       0x2CFFE7D5975E55E2L, 0x6E0F063E3EB46371L,
                                                       0xA91E2402C48A38C4L, 0xEBEEC5E96D600E57L,
                                                       0x65CC8190991CB93DL, 0x273C607B30F68FAEL,
                                                       0xE02D4247CAC8D41BL, 0xA2DDA3AC6322E288L,
                                                       0xBE992B5F8BDB8C5CL, 0xFC69CAB42231BACFL,
                                                       0x3B78E888D80FE17AL, 0x7988096371E5D7E9L,
                                                       0xF7AA4D1A85996083L, 0xB55AACF12C735610L,
                                                       0x724B8ECDD64D0DA5L, 0x30BB6F267FA73B36L,
                                                       0x4AC29F2A07BFD00DL, 0x08327EC1AE55E69EL,
                                                       0xCF235CFD546BBD2BL, 0x8DD3BD16FD818BB8L,
                                                       0x03F1F96F09FD3CD2L, 0x41011884A0170A41L,
                                                       0x86103AB85A2951F4L, 0xC4E0DB53F3C36767L,
                                                       0xD8A453A01B3A09B3L, 0x9A54B24BB2D03F20L,
                                                       0x5D45907748EE6495L, 0x1FB5719CE1045206L,
                                                       0x919735E51578E56CL, 0xD367D40EBC92D3FFL,
                                                       0x1476F63246AC884AL, 0x568617D9EF46BED9L,
                                                       0xE085162AB69D5E3CL, 0xA275F7C11F7768AFL,
                                                       0x6564D5FDE549331AL, 0x279434164CA30589L,
                                                       0xA9B6706FB8DFB2E3L, 0xEB46918411358470L,
                                                       0x2C57B3B8EB0BDFC5L, 0x6EA7525342E1E956L,
                                                       0x72E3DAA0AA188782L, 0x30133B4B03F2B111L,
                                                       0xF7021977F9CCEAA4L, 0xB5F2F89C5026DC37L,
                                                       0x3BD0BCE5A45A6B5DL, 0x79205D0E0DB05DCEL,
                                                       0xBE317F32F78E067BL, 0xFCC19ED95E6430E8L,
                                                       0x86B86ED5267CDBD3L, 0xC4488F3E8F96ED40L,
                                                       0x0359AD0275A8B6F5L, 0x41A94CE9DC428066L,
                                                       0xCF8B0890283E370CL, 0x8D7BE97B81D4019FL,
                                                       0x4A6ACB477BEA5A2AL, 0x089A2AACD2006CB9L,
                                                       0x14DEA25F3AF9026DL, 0x562E43B4931334FEL,
                                                       0x913F6188692D6F4BL, 0xD3CF8063C0C759D8L,
                                                       0x5DEDC41A34BBEEB2L, 0x1F1D25F19D51D821L,
                                                       0xD80C07CD676F8394L, 0x9AFCE626CE85B507L};
    private long crc = 0;

    @Override
    public void update(final int b) {
        update((byte) (b & 0xFF));
    }

    public void update(final byte b) {
        final int tab_index = ((int) (this.crc >> 56) ^ b) & 0xFF;
        this.crc = CRC_TABLE[tab_index] ^ (this.crc << 8);
    }

    @Override
    public void update(final byte[] buffer, final int offset, int length) {
        for (int i = offset; length > 0; length--) {
            update(buffer[i++]);
        }
    }

    @Override
    public void update(final byte[] buffer) {
        for (int i = 0; i < buffer.length; i++) {
            update(buffer[i]);
        }
    }

    @Override
    public long getValue() {
        return this.crc;
    }

    @Override
    public void reset() {
        this.crc = 0;
    }

}
