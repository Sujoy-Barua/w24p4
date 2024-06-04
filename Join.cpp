#include "Join.hpp"
#include <iostream>
#include <vector>
using namespace std;
/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel, pair<uint, uint> right_rel) {
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));

	// Process the left relation
	for (unsigned int page_id = left_rel.first; page_id < left_rel.second; ++page_id) {
		mem->loadFromDisk(disk, page_id, 0); // Using 0 as a buffer page
		Page* page = mem->mem_page(0);
		for (unsigned int record_id = 0; record_id < page->size(); ++record_id) {
			Record record = page->get_record(record_id);
			unsigned int index = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			// Should only use add_left_rel_page when the page is FULL
			if (mem->mem_page(index)->full()) {
				// flush page to disk
				unsigned int page_id = mem->flushToDisk(disk, index);
				partitions[index].add_left_rel_page(page_id);
				mem->mem_page(index)->loadRecord(record);
			} else {
				mem->mem_page(index)->loadRecord(record);
			}
		}
		page->reset(); // Clear the buffer page
	}
	// flush remaining pages to disk
	for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
		if (!mem->mem_page(i)->empty()) {
			unsigned int page_id = mem->flushToDisk(disk, i);
			partitions[i].add_left_rel_page(page_id);
		}
	}
	// Process the right relation in the same manner
	//unordered_map<uint, Page> right_partition_map;
	for (unsigned int page_id = right_rel.first; page_id < right_rel.second; ++page_id) {
		mem->loadFromDisk(disk, page_id, 0); // Using 0 as a buffer page
		Page* page = mem->mem_page(0);
		for (unsigned int record_id = 0; record_id < page->size(); ++record_id) {
			Record record = page->get_record(record_id);
			unsigned int index = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			// Should only use add_right_rel_page when the page is FULL
			if (mem->mem_page(index)->full()) {
				// flush page to disk
				unsigned int page_id = mem->flushToDisk(disk, index);
				partitions[index].add_right_rel_page(page_id);
				mem->mem_page(index)->loadRecord(record);
			} else {
				mem->mem_page(index)->loadRecord(record);
			}
		}
		page->reset(); // Clear the buffer page
	}
	// flush remaining pages to disk
	for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
		if (!mem->mem_page(i)->empty()) {
			unsigned int page_id = mem->flushToDisk(disk, i);
			partitions[i].add_right_rel_page(page_id);
		}
	}
	return partitions;
}
/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	vector<uint> result_pages;
	// used to store the join result
	Page* output_page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
	output_page->reset(); // Clear the output page initially
	for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
		Bucket bucket = partitions[i];
		auto left_pages = bucket.get_left_rel();
		auto right_pages = bucket.get_right_rel();
		// Clear sub-partition pages
		for (uint i = 0; i < MEM_SIZE_IN_PAGE - 2; i++) {
			mem->mem_page(i)->reset();
		}
		if (left_pages.size() < right_pages.size()) {
			// Load sub-partitions for left relation
			for (auto page_id : left_pages) {
				mem->loadFromDisk(disk, page_id, 1); // Load into a temporary buffer (1)
				Page* temp_page = mem->mem_page(1);
				for (unsigned int i = 0; i < temp_page->size(); i++) {
					Record record = temp_page->get_record(i);
					int index = record.probe_hash() % (MEM_SIZE_IN_PAGE - 2); // Create sub-partitions in B-2 pages
					mem->mem_page(index)->loadRecord(record); // Load record into its corresponding sub-partition
				}
				temp_page->reset(); // Clear the temporary buffer
			}
			// Compare with right relation
			for (auto page_id : right_pages) {
				mem->loadFromDisk(disk, page_id, 1); // Load into a temporary buffer (1)
				Page* temp_page = mem->mem_page(1);
				for (unsigned int i = 0; i < temp_page->size(); i++) {
					Record right_record = temp_page->get_record(i);
					int index = right_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					Page* sub_page = mem->mem_page(index);
					for (unsigned int j = 0; j < sub_page->size(); j++) {
						Record left_record = sub_page->get_record(j);
						if (left_record == right_record) {
							if (!output_page->full()) {
								output_page->loadPair(left_record, right_record); // Load matched pair into output pag
							} else {
								unsigned int output_page_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
								result_pages.push_back(output_page_id);
								output_page->reset();
								output_page->loadPair(left_record, right_record); // Load matched pair into output page
							}
						}
					}
				}
				temp_page->reset(); // Clear the temporary buffer
			}
		} else {
			// Load sub-partitions for right relation
			for (auto page_id : right_pages) {
				mem->loadFromDisk(disk, page_id, 1); // Load into a temporary buffer (1)
				Page* temp_page = mem->mem_page(1);
				for (unsigned int i = 0; i < temp_page->size(); i++) {
					Record record = temp_page->get_record(i);
					int index = record.probe_hash() % (MEM_SIZE_IN_PAGE - 2); // Create sub-partitions in B-2 pages
					mem->mem_page(index)->loadRecord(record); // Load record into its corresponding sub-partition
				}
				temp_page->reset(); // Clear the temporary buffer
			}
			// Compare with left relation
			for (auto page_id : left_pages) {
				mem->loadFromDisk(disk, page_id, 1); // Load into a temporary buffer (1)
				Page* temp_page = mem->mem_page(1);
				for (unsigned int i = 0; i < temp_page->size(); i++) {
					Record left_record = temp_page->get_record(i);
					int index = left_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					Page* sub_page = mem->mem_page(index);
					for (unsigned int j = 0; j < sub_page->size(); j++) {
						Record right_record = sub_page->get_record(j);
						if (left_record == right_record) {
							if (!output_page->full()) {
								output_page->loadPair(left_record, right_record); // Load matched pair into output page
							} else {
								unsigned int output_page_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
								result_pages.push_back(output_page_id);
								output_page->reset();
								output_page->loadPair(left_record, right_record); // Load matched pair into output page
							}
						}
					}
				}
				temp_page->reset(); // Clear the temporary buffer
			}
		}
		// Clean up all used sub-partition pages
		for (uint i = 0; i < MEM_SIZE_IN_PAGE - 2; i++) {
			mem->mem_page(i)->reset();
		}
	}
	// Flush remaining data in the output buffer if not empty
	if (!output_page->empty()) {
		unsigned int output_page_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
		result_pages.push_back(output_page_id);
		output_page->reset();
	}
	return result_pages;
}