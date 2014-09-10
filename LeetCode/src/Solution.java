/**
 * Definition for binary tree
* public class TreeNode {
*     int val;
*     TreeNode left;
*     TreeNode right;
*     TreeNode(int x) { val = x; }
* }
 */
public class Solution {
	public TreeNode buildTree(int[] preorder, int[] inorder) {
		return build(inorder, 0, inorder.length - 1, preorder, 0, preorder.length - 1);
	}

	public TreeNode build(int[] inorder, int lo1, int hi1,
						 	int[] preorder, int lo2, int hi2) {
		if (lo2 > hi2) return null;

		int index = getIndex(inorder, preorder[lo2]);

		TreeNode root = new TreeNode(preorder[lo2]);
		root.left = build(inorder, lo1, index - 1, preorder, lo2 + 1, lo2 + 1 + ((index - 1) - lo1));
		root.right = build(inorder, index + 1, hi1, preorder,
				lo2 + 1 + ((index - 1) - lo1) + 1, lo2 + 1 + ((index - 1) - lo1) + 1 + (hi1 - (index + 1)));

		return root;
	}

	public int getIndex(int[] array, int val) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] == val) return i;
		}

		return -1;
	}
}

class TreeNode {
	int val;
	TreeNode left;
	TreeNode right;
	TreeNode(int x) { val = x; }
}